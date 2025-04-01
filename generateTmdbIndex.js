const admin = require("firebase-admin");

// Inicializar Firebase con las credenciales
const serviceAccount = require("./firebase.json");

// Aumentar el límite de memoria de Node.js
// Puede ejecutar este script con: node --max-old-space-size=8192 script.js
// O modificar la cantidad según sus necesidades (4096, 8192, 16384)

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://bubbo-dfba0-default-rtdb.europe-west1.firebasedatabase.app/"
});

const database = admin.database();

// Ruta en Realtime Database donde están los JSONL
const DB_PATH = "documents/Content/latest";

async function generarIndiceTmdb() {
  console.log("Iniciando indexación de TMDB IDs...");
  const BATCH_SIZE = 3; // Reducido a 3 plataformas a la vez para menor uso de memoria
  let lastProcessedKey = null;

  try {
    let hasMore = true;

    while (hasMore) {
      console.log(`Obteniendo siguiente lote de plataformas después de: ${lastProcessedKey || 'inicio'}...`);
      let query = database.ref(DB_PATH).orderByKey();
      
      if (lastProcessedKey) {
        query = query.startAt(lastProcessedKey + '\uf8ff');
      }
      
      const platformsSnapshot = await query.limitToFirst(BATCH_SIZE).once("value");
      
      if (!platformsSnapshot.exists()) {
        console.log('No hay más plataformas para procesar.');
        hasMore = false;
        continue;
      }

      const platforms = [];
      platformsSnapshot.forEach((child) => {
        platforms.push(child);
        lastProcessedKey = child.key;
      });

      if (platforms.length < BATCH_SIZE) {
        hasMore = false;
      }

      // Procesar cada plataforma en el lote actual
      for (const platformSnapshot of platforms) {
        const platformKey = platformSnapshot.key;
        const countryProvider = platformKey.replace(/_jsonl$/, '');
        console.log(`Procesando ${countryProvider}...`);
        
        // Crear un índice específico para esta plataforma
        const tmdbIndexForPlatform = {};

        try {
          // Procesar en sublotes para reducir uso de memoria
          await processPlatformInChunks(platformKey, countryProvider, tmdbIndexForPlatform);
          
          // Liberar memoria
          global.gc && global.gc();
          
        } catch (error) {
          console.error(`Error procesando ${platformKey}:`, error);
        }
      }
    }

    console.log("Proceso de indexación completado con éxito 🚀");

  } catch (error) {
    console.error("Error durante la generación del índice:", error);
    throw error;
  }
}

// Función para procesar una plataforma en fragmentos más pequeños
async function processPlatformInChunks(platformKey, countryProvider, tmdbIndexForPlatform) {
  const CONTENT_CHUNK_SIZE = 100; // Procesar 100 entradas a la vez
  let contentProcessed = 0;
  let hasMoreContent = true;
  let lastProcessedContentKey = null;
  
  while (hasMoreContent) {
    // Consultar un subconjunto del contenido
    let contentQuery = database.ref(`${DB_PATH}/${platformKey}/content`).orderByKey();
    
    if (lastProcessedContentKey) {
      contentQuery = contentQuery.startAt(lastProcessedContentKey + '\uf8ff');
    }
    
    const contentChunkSnapshot = await contentQuery.limitToFirst(CONTENT_CHUNK_SIZE).once("value");
    
    if (!contentChunkSnapshot.exists()) {
      console.log(`Sin más contenido para ${platformKey}`);
      hasMoreContent = false;
      continue;
    }
    
    const entries = [];
    contentChunkSnapshot.forEach((child) => {
      entries.push(child.val());
      lastProcessedContentKey = child.key;
    });
    
    if (entries.length < CONTENT_CHUNK_SIZE) {
      hasMoreContent = false;
    }
    
    contentProcessed += entries.length;
    console.log(`Procesando fragmento de ${entries.length} entradas para ${platformKey} (total: ${contentProcessed})`);
    
    // Procesar este fragmento de entradas
    for (const entry of entries) {
      if (!entry || !entry.ExternalIds) continue;

      for (const idObj of entry.ExternalIds) {
        if (idObj.Provider === "tmdb" && idObj.ID) {
          const tmdbId = idObj.ID;
          
          // Validar y asignar valores por defecto para evitar undefined
          const deeplinks = entry.Deeplinks || {};
          const title = entry.Title || "";
          const year = entry.Year || null;
          const platformCode = entry.PlatformCode || "";
          const platformCountry = entry.PlatformCountry || "";

          // Solo agregar al índice si todos los valores son válidos
          if (Object.keys(deeplinks).length > 0 && title) {
            tmdbIndexForPlatform[tmdbId] = {
              deeplinks,
              title,
              year,
              platformCode,
              platformCountry
            };
          }
        }
      }
    }
    
    // Liberar memoria después de procesar cada fragmento
    entries.length = 0;
    global.gc && global.gc();
  }
  
  // Guardar el índice de esta plataforma en Firebase
  if (Object.keys(tmdbIndexForPlatform).length > 0) {
    try {
      // Actualizar en fragmentos si es necesario para evitar problemas de memoria
      if (Object.keys(tmdbIndexForPlatform).length > 1000) {
        console.log(`El índice para ${countryProvider} es grande, actualizando en fragmentos...`);
        await updateIndexInChunks("tmdb_index", countryProvider, tmdbIndexForPlatform);
      } else {
        await database.ref("tmdb_index").update({
          [countryProvider]: tmdbIndexForPlatform
        });
      }
      console.log(`Índice actualizado para ${countryProvider} con ${Object.keys(tmdbIndexForPlatform).length} entradas`);
    } catch (error) {
      console.error(`Error al actualizar ${countryProvider}:`, error.message);
    }
  } else {
    console.log(`No hay datos válidos para actualizar en ${countryProvider}`);
  }
}

// Función para actualizar un índice grande en fragmentos
async function updateIndexInChunks(indexPath, countryProvider, indexData) {
  const CHUNK_SIZE = 500; // Número de entradas por actualización
  const keys = Object.keys(indexData);
  
  for (let i = 0; i < keys.length; i += CHUNK_SIZE) {
    const chunkKeys = keys.slice(i, i + CHUNK_SIZE);
    const chunkData = {};
    
    chunkKeys.forEach(key => {
      chunkData[key] = indexData[key];
    });
    
    console.log(`Actualizando fragmento ${i/CHUNK_SIZE + 1}/${Math.ceil(keys.length/CHUNK_SIZE)} para ${countryProvider}`);
    await database.ref(`${indexPath}/${countryProvider}`).update(chunkData);
    
    // Liberar memoria después de cada actualización
    global.gc && global.gc();
  }
}

// Ejecutar con manejo de errores
generarIndiceTmdb().catch(error => {
  console.error("Error en la ejecución principal:", error);
  process.exit(1);
});