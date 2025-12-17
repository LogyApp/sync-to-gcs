const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const express = require('express');
const { Firestore } = require('@google-cloud/firestore');
const { PubSub } = require('@google-cloud/pubsub');
const fs = require('fs');
const app = express();
const path = require('path');

// Configuración
const GOOGLE_CLOUD_PROJECT = process.env.GOOGLE_CLOUD_PROJECT || "eternal-brand-454501-i8";
const BUCKET_NAME = process.env.BUCKET_NAME || "talenthub_central";
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD";
const PORT = process.env.PORT || 8080;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SYNC_TOPIC = process.env.SYNC_TOPIC || "drive-sync-topic";

const logyserSync = require('./foto_evidencias/evidencias.controller')

const LOCAL_CREDENTIALS_PATH = './gcs-key.json';

console.log('🔧 Inicializando servicios de Google Cloud...');


let storage, firestore, pubsub;

// Colección para almacenar estado de sincronización
const SYNC_COLLECTION = 'drive_sync_state';
const WEBHOOK_COLLECTION = 'drive_webhooks';

// Para evitar procesamiento duplicado de notificaciones
const processedChanges = new Set();
const CHANGE_TTL = 300000; // 5 minutos

const POLLING_INTERVAL = 30000;

// Middleware
app.use(express.json());

/**
 * Procesa archivos en PARALELO (5-10 a la vez)
 */
async function processFilesInParallel(files, prefix, token, maxParallel = 10) {
    const results = { ok: 0, fail: 0 };
    const semaphore = { count: 0 }; // Controlar concurrencia

    console.log(`   🔄 Procesando ${files.length} archivos en paralelo (${maxParallel} concurrentes)`);

    const processFile = async (file) => {
        // Esperar si hay demasiados procesos concurrentes
        while (semaphore.count >= maxParallel) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        semaphore.count++;
        try {
            const blob = await downloadDriveFileREST(file.id, file.mimeType, token);
            const objectName = prefix + file.name;
            await uploadBlobToGCS(BUCKET_NAME, objectName, blob, file.mimeType);
            results.ok++;
            console.log(`   ✅ [${results.ok}/${files.length}] ${file.name}`);
        } catch (error) {
            results.fail++;
            console.log(`   ❌ Error con ${file.name}: ${error.message}`);
        } finally {
            semaphore.count--;
        }
    };

    // Iniciar TODOS los procesos (JavaScript maneja la concurrencia)
    const promises = files.map(file => processFile(file));
    await Promise.all(promises);

    return results;
}

function checkLocalCredentials() {
    const credsPath = LOCAL_CREDENTIALS_PATH;
    if (fs.existsSync(credsPath)) {
        try {
            const creds = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
            console.log('✅ Credenciales locales encontradas');
            console.log(`   Tipo: ${creds.type || 'N/A'}`);
            console.log(`   Cliente ID: ${creds.client_id?.substring(0, 20)}...`);
            console.log(`   Proyecto: ${creds.project_id || 'N/A'}`);
            return true;
        } catch (error) {
            console.error('❌ Error leyendo credenciales:', error.message);
            return false;
        }
    } else {
        console.log('⚠️  No se encontró gcs-key.json en la raíz del proyecto');
        console.log('📌 El polling y sincronización manual fallarán en local');
        return false;
    }
}

function createMockFirestore() {
    console.log('🎭 Usando Firestore mock para desarrollo');

    // Cargar datos desde archivo si existe
    let mockData = {};
    const DATA_FILE = './mock-firestore-data.json';

    try {
        if (fs.existsSync(DATA_FILE)) {
            mockData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
            console.log('📂 Datos de mock cargados desde archivo');
        }
    } catch (e) {
        console.log('📝 Creando nuevo almacenamiento mock');
    }

    // Inicializar con valores por defecto si no existen
    if (!mockData['drive_sync_state/last_sync']) {
        mockData['drive_sync_state/last_sync'] = {
            timestamp: '2000-01-01T00:00:00.000Z',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        };
    }

    return {
        collection: (collectionName) => ({
            doc: (docId) => ({
                get: async () => {
                    const key = `${collectionName}/${docId}`;
                    const exists = mockData[key] !== undefined;
                    console.log(`📄 Mock Firestore - GET ${key}: ${exists ? 'EXISTE' : 'NO EXISTE'}`);

                    return {
                        exists,
                        data: () => mockData[key]
                    };
                },
                set: async (data) => {
                    const key = `${collectionName}/${docId}`;
                    mockData[key] = { ...mockData[key], ...data };
                    console.log(`💾 Mock Firestore - SET ${key}:`, data.timestamp || data);

                    // Guardar en archivo para persistencia
                    try {
                        fs.writeFileSync(DATA_FILE, JSON.stringify(mockData, null, 2), 'utf8');
                    } catch (e) {
                        console.warn('⚠️  No se pudo guardar mock data:', e.message);
                    }
                }
            })
        }),
        listCollections: async () => {
            console.log('📋 Mock Firestore: Listando colecciones');
            return [];
        }
    };
}

function startDrivePolling() {
    console.log(`🔄 Configurando polling automático cada ${POLLING_INTERVAL / 1000} segundos...`);

    // Verificar credenciales en local
    if (!process.env.K_SERVICE) {
        const hasCreds = checkLocalCredentials();
        if (!hasCreds) {
            console.error('❌ No hay credenciales locales, polling no funcionará');
            return;
        }
    }

    // Iniciar después de 5 segundos
    setTimeout(() => {
        runPollingCycle().catch(error => {
            console.error('❌ Error fatal en polling:', error);
        });
    }, 5000);
}

// async function initializeFirestoreWithRetry() {
//     // SI ESTAMOS EN LOCAL, USAR UN MOCK
//     if (process.env.NODE_ENV !== 'production' && !process.env.K_SERVICE) {
//         console.log('🔧 MODO DESARROLLO LOCAL - Usando mock de Firestore');
//         return createMockFirestore();
//     }

//     const maxRetries = 3;

//     for (let attempt = 1; attempt <= maxRetries; attempt++) {
//         try {
//             console.log(`Intento ${attempt}/${maxRetries} de inicializar Firestore...`);

//             const firestoreConfig = {
//                 projectId: GOOGLE_CLOUD_PROJECT,
//                 ignoreUndefinedProperties: true
//             };

//             console.log(`📁 Proyecto Firestore: ${GOOGLE_CLOUD_PROJECT}`);

//             const firestoreInstance = new Firestore(firestoreConfig);
//             await firestoreInstance.listCollections();

//             console.log('✅ Firestore inicializado correctamente');
//             return firestoreInstance;

//         } catch (error) {
//             console.error(`Intento ${attempt} fallado:`, error.message);

//             if (attempt === maxRetries) {
//                 console.error('❌ Todos los intentos fallaron.');
//                 return createMockFirestore();
//             }

//             await new Promise(resolve => setTimeout(resolve, 1000));
//         }
//     }
// }

async function initializeFirestoreWithRetry() {
    const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
    const KEY_FILE_PATH = path.resolve(LOCAL_CREDENTIALS_PATH);

    console.log(`🔧 Inicializando Firestore...`);
    console.log(`   📍 Modo: ${IS_LOCAL ? 'Local' : 'Cloud'}`);

    try {
        if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
            console.log('🔑 Usando credenciales locales para Firestore');

            // Leer credenciales
            const keyContent = JSON.parse(fs.readFileSync(KEY_FILE_PATH, 'utf8'));

            return new Firestore({
                projectId: keyContent.project_id || GOOGLE_CLOUD_PROJECT,
                keyFilename: KEY_FILE_PATH,
                ignoreUndefinedProperties: true
            });
        } else {
            console.log('🌐 Usando Application Default Credentials para Firestore');

            // Configurar variable de entorno si existe
            if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
                process.env.GOOGLE_APPLICATION_CREDENTIALS = KEY_FILE_PATH;
            }

            return new Firestore({
                projectId: GOOGLE_CLOUD_PROJECT,
                ignoreUndefinedProperties: true
            });
        }
    } catch (error) {
        console.error('❌ Error inicializando Firestore:', error.message);

        // Fallback: usar mock
        console.log('🎭 Usando Firestore mock como fallback');
        return createMockFirestore();
    }
}

async function initializeStorageWithRetry() {
    const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
    const KEY_FILE_PATH = path.resolve(LOCAL_CREDENTIALS_PATH);

    console.log(`🔧 Inicializando Google Cloud Storage...`);
    console.log(`   📍 Modo: ${IS_LOCAL ? 'Local' : 'Cloud'}`);
    console.log(`   📍 Ruta credenciales: ${KEY_FILE_PATH}`);
    console.log(`   📍 Existe archivo: ${fs.existsSync(KEY_FILE_PATH)}`);

    // OPCIÓN 1: Usar credenciales específicas si estamos en local
    if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
        console.log('🔑 Usando credenciales locales para Storage');
        try {
            // Leer y validar credenciales
            const keyContent = JSON.parse(fs.readFileSync(KEY_FILE_PATH, 'utf8'));
            console.log(`   📧 Cuenta: ${keyContent.client_email}`);
            console.log(`   🏢 Proyecto: ${keyContent.project_id}`);

            return new Storage({
                projectId: keyContent.project_id || GOOGLE_CLOUD_PROJECT,
                keyFilename: KEY_FILE_PATH
            });
        } catch (error) {
            console.error('❌ Error con credenciales locales:', error.message);
            // Continuar con método 2
        }
    }

    // OPCIÓN 2: Usar Application Default Credentials
    console.log('🌐 Usando Application Default Credentials para Storage');
    try {
        // Configurar variable de entorno para ADC
        if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
            process.env.GOOGLE_APPLICATION_CREDENTIALS = KEY_FILE_PATH;
            console.log(`   🔧 Estableciendo GOOGLE_APPLICATION_CREDENTIALS: ${KEY_FILE_PATH}`);
        }

        const storageInstance = new Storage({
            projectId: GOOGLE_CLOUD_PROJECT
        });

        // Verificar que funciona
        const [buckets] = await storageInstance.getBuckets();
        console.log(`✅ Storage inicializado. Buckets disponibles: ${buckets.length}`);
        return storageInstance;

    } catch (error) {
        console.error('❌ Error con Application Default Credentials:', error.message);

        // OPCIÓN 3: Usar autenticación directa con GoogleAuth
        console.log('🔄 Intentando autenticación directa...');
        try {
            const auth = new GoogleAuth({
                keyFile: KEY_FILE_PATH,
                scopes: ['https://www.googleapis.com/auth/cloud-platform']
            });

            const client = await auth.getClient();
            const projectId = await auth.getProjectId();

            console.log(`   🔑 Autenticado como proyecto: ${projectId}`);

            return new Storage({
                projectId: projectId,
                authClient: client
            });

        } catch (authError) {
            console.error('❌ Todas las opciones de autenticación fallaron:', authError.message);
            throw new Error('No se pudo autenticar con Google Cloud Storage');
        }
    }
}

async function initializeGoogleCloudServices() {
    try {
        console.log('🚀 Inicializando servicios Google Cloud...');

        // 🔥 DETECTAR ENTORNO
        const IS_CLOUD_RUN = process.env.K_SERVICE !== undefined;
        console.log(`   📍 Entorno: ${IS_CLOUD_RUN ? 'Cloud Run' : 'Local'}`);
        console.log(`   📍 Proyecto: ${GOOGLE_CLOUD_PROJECT}`);

        // 1. STORAGE
        storage = await initializeStorageWithRetry();
        console.log('✅ Storage inicializado');

        // 2. FIRESTORE
        firestore = await initializeFirestoreWithRetry();
        console.log('✅ Firestore inicializado');

        // 3. PUBSUB
        if (IS_CLOUD_RUN) {
            pubsub = new PubSub({ projectId: GOOGLE_CLOUD_PROJECT });
        } else if (fs.existsSync(LOCAL_CREDENTIALS_PATH)) {
            pubsub = new PubSub({
                projectId: GOOGLE_CLOUD_PROJECT,
                keyFilename: LOCAL_CREDENTIALS_PATH
            });
        } else {
            pubsub = { topic: () => ({ publishMessage: async () => { } }) };
        }
        console.log('✅ PubSub inicializado');

        // Verificar bucket
        console.log(`🔍 Verificando bucket: ${BUCKET_NAME}`);
        try {
            const [bucketExists] = await storage.bucket(BUCKET_NAME).exists();
            if (!bucketExists) {
                console.log(`🆕 Bucket no existe, creando: ${BUCKET_NAME}`);
                await storage.createBucket(BUCKET_NAME, {
                    location: 'us-central1',
                    storageClass: 'STANDARD'
                });
                console.log(`✅ Bucket creado: ${BUCKET_NAME}`);
            } else {
                console.log(`✅ Bucket existe: ${BUCKET_NAME}`);
            }
        } catch (bucketError) {
            console.error(`⚠️  Error con bucket: ${bucketError.message}`);
        }

        console.log('🎉 Todos los servicios inicializados correctamente');
        return true;

    } catch (error) {
        console.error('❌ Error inicializando servicios:', error.message);

        // Fallback mínimo
        console.log('⚠️  Usando servicios de fallback limitados');

        try {
            storage = new Storage({ projectId: GOOGLE_CLOUD_PROJECT });
        } catch (e) {
            storage = null;
        }

        firestore = createMockFirestore();
        pubsub = { topic: () => ({ publishMessage: async () => { } }) };

        return false;
    }
}

/**
 * Obtiene último tiempo de sincronización desde Firestore
 * CON MANEJO DE ERRORES ESPECÍFICO PARA CLOUD RUN
 */
async function getLastSyncTime() {
    const DEFAULT_TIME = '2000-01-01T00:00:00.000Z';

    // Si no hay firestore inicializado, usar valor por defecto
    if (!firestore) {
        console.log('⚠️  Firestore no inicializado, usando valor por defecto');
        return DEFAULT_TIME;
    }

    try {
        const doc = await firestore.collection(SYNC_COLLECTION).doc('last_sync').get();

        if (doc.exists && doc.data().timestamp) {
            return doc.data().timestamp;
        } else {
            // Crear documento si no existe
            await setLastSyncTime(DEFAULT_TIME);
            return DEFAULT_TIME;
        }

    } catch (error) {
        // 🔥 MANEJO MEJORADO PARA ERRORES DE FIRESTORE
        console.error(`🔴 ERROR Firestore en getLastSyncTime:`, {
            code: error.code,
            message: error.message,
            time: new Date().toISOString()
        });

        // Si es error de "no encontrado" o permisos, usar valor por defecto
        if (error.code === 5 || error.code === 7 || error.code === 16) {
            console.log('📝 Usando valor por defecto debido a error Firestore');
            return DEFAULT_TIME;
        }

        // Para otros errores, propagar
        throw error;
    }
}

/**
 * Guarda último tiempo de sincronización en Firestore
 */
async function setLastSyncTime(timestamp) {
    try {
        console.log(`🔄 Intentando guardar lastSyncTime: ${timestamp}`); // <-- AGREGAR
        console.log(`📝 Tipo de timestamp: ${typeof timestamp}, Valor: ${timestamp}`); // <-- AGREGAR

        await firestore.collection(SYNC_COLLECTION).doc('last_sync').set({
            timestamp: timestamp,
            updatedAt: new Date().toISOString()
        });

        console.log(`✅ lastSyncTime guardado exitosamente: ${timestamp}`); // <-- AGREGAR
    } catch (error) {
        console.error('Error guardando lastSyncTime:', error);
    }
}

/**
 * Renueva automáticamente los webhooks antes de que expiren
 */
async function renewWebhooks() {
    try {
        const snapshot = await firestore.collection(WEBHOOK_COLLECTION).get();

        for (const doc of snapshot.docs) {
            const webhookData = doc.data();
            const expirationTime = parseInt(webhookData.expiration);

            // Renovar si expira en menos de 4 horas
            if (expirationTime - Date.now() < 4 * 60 * 60 * 1000) {
                console.log('🔄 Renovando webhook que expira pronto:', webhookData.id);

                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });

                const client = await auth.getClient();
                const drive = google.drive({ version: 'v3', auth: client });

                // Obtener token de página inicial
                const startPageToken = await drive.changes.getStartPageToken();

                // Renovar webhook
                await drive.channels.stop({
                    requestBody: {
                        id: webhookData.id,
                        resourceId: webhookData.resourceId
                    }
                });

                const newWebhook = await drive.changes.watch({
                    pageToken: startPageToken.data.startPageToken,
                    requestBody: {
                        id: webhookData.id,
                        type: 'web_hook',

                        address: `${WEBHOOK_URL}/sync/webhook`,
                        expiration: (Date.now() + 86400000).toString(), // 24 horas
                    }
                });

                // Actualizar en Firestore
                await firestore.collection(WEBHOOK_COLLECTION).doc(webhookData.id).set({
                    id: newWebhook.data.id,
                    resourceId: newWebhook.data.resourceId,
                    expiration: newWebhook.data.expiration,
                    address: newWebhook.data.address,
                    updatedAt: new Date().toISOString()
                });

                console.log('✅ Webhook renovado:', newWebhook.data.id);
            }
        }
    } catch (error) {
        console.error('❌ Error renovando webhooks:', error.message);
    }
}

/**
 * Configuración inicial del webhook de Drive
 */
async function setupDriveWebhook() {
    try {
        if (!WEBHOOK_URL) {
            console.log('⚠️  WEBHOOK_URL no configurada. Solo funcionará polling');
            return;
        }

        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const drive = google.drive({ version: 'v3', auth: client });

        // Obtener token de página inicial
        const startPageToken = await drive.changes.getStartPageToken();
        console.log('🔑 Token de página inicial:', startPageToken.data.startPageToken);

        // Generar ID único para el webhook
        const webhookId = 'drive-to-gcs-sync-webhook-' + Date.now();

        // Configurar webhook
        const response = await drive.changes.watch({
            pageToken: startPageToken.data.startPageToken,
            requestBody: {
                id: webhookId,
                type: 'web_hook',
                address: `${WEBHOOK_URL}/sync/webhook`,
                expiration: (Date.now() + 86400000).toString(), // 24 horas
            }
        });

        // Guardar información del webhook en Firestore para renovación automática
        await firestore.collection(WEBHOOK_COLLECTION).doc(webhookId).set({
            id: response.data.id,
            resourceId: response.data.resourceId,
            expiration: response.data.expiration,
            address: response.data.address,
            createdAt: new Date().toISOString()
        });

        console.log('✅ Webhook de Drive configurado exitosamente!');
        console.log('📋 Resource ID:', response.data.resourceId);
        console.log('🌐 Drive notificará a:', WEBHOOK_URL);
        console.log('⏰ Expira:', new Date(parseInt(response.data.expiration)).toLocaleString());

    } catch (error) {
        console.error('❌ Error configurando webhook:', error.message);
        if (error.response?.data) {
            console.error('Detalles del error:', error.response.data);
        }
    }
}

/**
 * Lista archivos en carpeta con query personalizable
 */
async function listFilesInFolderREST(folderId, token, customQuery) {
    const files = [];
    let pageToken = null;
    const q = customQuery || `'${folderId}' in parents and trashed = false`;

    do {
        // 🔥 OBTENER MÁS CAMPOS: incluir size, webContentLink, etc.
        const url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}` +
            `&fields=nextPageToken,files(id,name,mimeType,modifiedTime,createdTime,size,webContentLink,webViewLink,iconLink,parents,trashed)` +
            `&pageSize=1000` + // 🔥 Máximo permitido
            (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");

        console.log(`   📤 Consultando: ${folderId} (página ${pageToken ? 'siguiente' : '1'})`);

        const response = await fetch(url, {
            headers: { Authorization: "Bearer " + token },
        });

        if (!response.ok) {
            console.error(`   ❌ Error API: ${response.status}`);
            // 🔥 NO LANZAR ERROR: Devolver lo que tengamos
            break;
        }

        const data = await response.json();
        if (data.files && data.files.length) {
            files.push(...data.files);
            console.log(`   📥 Obtenidos ${data.files.length} archivos (total: ${files.length})`);
        }
        pageToken = data.nextPageToken || null;

    } while (pageToken);

    console.log(`   📊 Total final: ${files.length} archivos`);
    return files;
}

async function downloadDriveFileREST(fileId, mimeType, token) {
    let url;

    // 🔥 QUITAR FILTROS: Manejar TODOS los tipos de Google Apps
    if (mimeType && mimeType.includes("application/vnd.google-apps")) {
        // Exportar cualquier Google Doc/Sheet/Slide a PDF
        url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}/export?mimeType=application/pdf`;
    } else {
        // Cualquier otro archivo: descargar directamente
        url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`;
    }

    console.log(`   📥 URL de descarga: ${url.substring(0, 100)}...`);

    const response = await fetch(url, {
        headers: { Authorization: "Bearer " + token },
    });

    if (!response.ok) {
        const errorText = await response.text();
        console.error(`   ❌ Error descarga ${response.status}: ${errorText.substring(0, 200)}`);
        throw new Error(`Drive download error ${response.status}`);
    }

    const buffer = await response.buffer();
    console.log(`   📥 Descargado: ${buffer.length} bytes`);
    return buffer;
}

/**
 * Sube blob a Google Cloud Storage
 */
async function uploadBlobToGCS(bucket, objectName, blob, contentType) {
    const MAX_RETRIES = 5; // 🔥 Aumentar reintentos
    let lastError = null;

    // 🔥 QUITAR VALIDACIONES: Aceptar cualquier tipo de contenido
    if (!contentType || contentType === '') {
        contentType = 'application/octet-stream'; // Tipo por defecto
    }

    // 🔥 Sanitizar nombre de archivo (remover caracteres problemáticos)
    const sanitizedObjectName = objectName
        .replace(/[^\w\-\/\.\s]/g, '_') // Reemplazar caracteres especiales
        .replace(/\s+/g, '_'); // Reemplazar espacios

    if (sanitizedObjectName !== objectName) {
        console.log(`   🔧 Nombre sanitizado: ${objectName} → ${sanitizedObjectName}`);
    }

    console.log(`   📦 Subiendo: ${sanitizedObjectName}`);
    console.log(`   📊 Tamaño: ${(blob.length / (1024 * 1024)).toFixed(2)} MB`);
    console.log(`   🏷️  Tipo: ${contentType}`);

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            console.log(`   🔄 Intento ${attempt}/${MAX_RETRIES}`);

            if (!storage) {
                console.log('   🔄 Re-inicializando Storage...');
                storage = await initializeStorageWithRetry();
            }

            const file = storage.bucket(bucket).file(sanitizedObjectName);

            // 🔥 CONFIGURACIÓN SIN RESTRICCIONES
            const options = {
                metadata: {
                    contentType: contentType,
                    // 🔥 Quitar validaciones estrictas
                },
                validation: false, // 🔥 Deshabilitar validación
                // 🔥 Para archivos grandes, usar upload resumible automáticamente
                resumable: blob.length > 5 * 1024 * 1024, // > 5MB = resumible
                // 🔥 Aumentar timeout para archivos grandes
                timeout: blob.length > 50 * 1024 * 1024 ? 600000 : 300000, // 10 o 5 minutos
            };

            // 🔥 PARA ARCHIVOS MUY GRANDES: usar upload en chunks
            if (blob.length > 100 * 1024 * 1024) { // > 100MB
                console.log(`   ⚠️  Archivo muy grande (${(blob.length / (1024 * 1024)).toFixed(2)} MB), usando upload optimizado`);

                // Opción 1: Usar stream para archivos muy grandes
                const writeStream = file.createWriteStream(options);

                return new Promise((resolve, reject) => {
                    writeStream.on('error', reject);
                    writeStream.on('finish', () => {
                        console.log(`   ✅ Archivo grande subido: ${sanitizedObjectName}`);
                        resolve(file);
                    });

                    // Escribir en chunks
                    const chunkSize = 10 * 1024 * 1024; // 10MB chunks
                    for (let i = 0; i < blob.length; i += chunkSize) {
                        const chunk = blob.slice(i, i + chunkSize);
                        writeStream.write(chunk);
                        console.log(`   📦 Chunk ${Math.floor(i / chunkSize) + 1} de ${Math.ceil(blob.length / chunkSize)}: ${(chunk.length / (1024 * 1024)).toFixed(2)} MB`);
                    }
                    writeStream.end();
                });
            }

            // Para archivos normales: upload directo
            await file.save(blob, options);

            console.log(`   ✅ Subido exitosamente: ${sanitizedObjectName}`);

            // Verificar que existe
            const [exists] = await file.exists();
            if (exists) {
                const [metadata] = await file.getMetadata();
                console.log(`   📅 Creado: ${metadata.timeCreated}`);
                console.log(`   🔗 URI: gs://${bucket}/${sanitizedObjectName}`);
                console.log(`   💾 Tamaño final: ${metadata.size} bytes`);
            }

            return file;

        } catch (error) {
            lastError = error;
            console.error(`   ❌ Intento ${attempt} fallado: ${error.message}`);

            // Análisis del error
            if (error.code === 400) {
                console.log('   🔧 Posible problema con el tipo de contenido, intentando con tipo genérico...');
                // Reintentar con tipo genérico
                contentType = 'application/octet-stream';
            }
            else if (error.code === 403) {
                console.log('   🔐 Error de permisos, esperando y reintentando...');
                await new Promise(resolve => setTimeout(resolve, 5000 * attempt));
            }
            else if (error.message.includes('timeout') || error.message.includes('socket')) {
                console.log(`   ⏱️  Timeout, aumentando tiempo de espera...`);
                await new Promise(resolve => setTimeout(resolve, 10000 * attempt));
            }
            else {
                console.log(`   🔄 Reintentando en ${2 * attempt} segundos...`);
                await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
            }
        }
    }

    // Si llegamos aquí, todos los intentos fallaron
    console.error(`   ❌ ERROR CRÍTICO: No se pudo subir ${objectName}`);
    console.error(`   📝 Último error: ${lastError?.message}`);

    // 🔥 NO LANZAR ERROR: Continuar con el siguiente archivo
    console.log(`   ⏭️  Saltando archivo y continuando...`);
    return null;
}

/**
 * Verifica si una carpeta está completamente vacía
 */
async function isFolderEmpty(folderId, token) {
    const q = `'${folderId}' in parents and trashed = false`;
    const items = await listFilesInFolderREST(folderId, token, q);
    return items.length === 0;
}

/**
 * Procesa carpetas recursivamente solo con archivos modificados
 */
async function processFolderIncremental(folderId, prefix, token, modifiedSince) {
    let ok = 0, fail = 0, folders = 0;

    console.log(`\n📁 PROCESANDO: ${prefix || 'raíz'}`);

    try {
        const q = `'${folderId}' in parents and trashed = false`;
        const items = await listFilesInFolderREST(folderId, token, q);

        console.log(`📊 Encontrados: ${items.length} items`);

        // Separar carpetas y archivos
        const folderItems = items.filter(item => item.mimeType === "application/vnd.google-apps.folder");
        const fileItems = items.filter(item => item.mimeType !== "application/vnd.google-apps.folder");

        console.log(`   📄 Archivos: ${fileItems.length}, 📁 Carpetas: ${folderItems.length}`);

        // 🔥 PROCESAR ARCHIVOS EN PARALELO
        if (fileItems.length > 0) {
            const fileResults = await processFilesInParallel(fileItems, prefix, token, 15); // 15 concurrentes
            ok += fileResults.ok;
            fail += fileResults.fail;
        }

        // 🔥 PROCESAR CARPETAS EN PARALELO (limitado a 3-5 para no saturar)
        const MAX_PARALLEL_FOLDERS = 5;
        console.log(`   🔄 Procesando ${folderItems.length} carpetas (${MAX_PARALLEL_FOLDERS} concurrentes)`);

        for (let i = 0; i < folderItems.length; i += MAX_PARALLEL_FOLDERS) {
            const batch = folderItems.slice(i, i + MAX_PARALLEL_FOLDERS);
            const batchPromises = batch.map(async (folder) => {
                console.log(`   📁 Iniciando: ${folder.name}`);
                folders++;
                const subPrefix = prefix + folder.name + "/";
                try {
                    const subStats = await processFolderIncremental(folder.id, subPrefix, token, modifiedSince);
                    return subStats;
                } catch (err) {
                    console.error(`   ❌ Error en carpeta ${folder.name}: ${err.message}`);
                    return { ok: 0, fail: 1, folders: 1 };
                }
            });

            const batchResults = await Promise.all(batchPromises);
            batchResults.forEach(stats => {
                ok += stats.ok;
                fail += stats.fail;
                folders += stats.folders;
            });

            console.log(`   📈 Progreso carpetas: ${Math.min(i + MAX_PARALLEL_FOLDERS, folderItems.length)}/${folderItems.length}`);
        }

        console.log(`\n✅ FINALIZADO: ${prefix || 'raíz'}`);
        console.log(`   ✅ Archivos: ${ok}, ❌ Fallos: ${fail}, 📁 Carpetas: ${folders}`);

        return { ok, fail, folders };

    } catch (error) {
        console.error(`❌ ERROR en ${prefix}: ${error.message}`);
        return { ok, fail, folders };
    }
}

/**
 * Procesa cambios en tiempo real con manejo de duplicados
 */
async function processRealTimeChange(changeId, fileId, resourceState) {
    // Evitar procesamiento duplicado
    if (processedChanges.has(changeId)) {
        console.log('⏭️  Cambio ya procesado:', changeId);
        return;
    }

    // Agregar a procesados con TTL
    processedChanges.add(changeId);
    setTimeout(() => processedChanges.delete(changeId), CHANGE_TTL);

    try {
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        // Obtener información del archivo modificado
        const driveResponse = await fetch(
            `https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType,modifiedTime,parents,trashed`,
            { headers: { Authorization: `Bearer ${token}` } }
        );

        if (!driveResponse.ok) {
            throw new Error(`Error obteniendo información del archivo: ${driveResponse.status}`);
        }

        const file = await driveResponse.json();

        // Si el archivo está en papelera, eliminarlo de GCS
        if (file.trashed) {
            try {
                // Intentar eliminar de GCS
                const fileName = file.name;
                await storage.bucket(BUCKET_NAME).file(fileName).delete();
                console.log(`🗑️  Archivo eliminado de GCS: ${fileName}`);
            } catch (deleteError) {
                if (deleteError.code === 404) {
                    console.log(`⚠️  Archivo no encontrado en GCS para eliminar: ${file.name}`);
                } else {
                    throw deleteError;
                }
            }
            return;
        }

        console.log(`📤 Sincronizando: ${file.name} (${resourceState})`);

        // Si es una carpeta, procesar recursivamente
        if (file.mimeType === "application/vnd.google-apps.folder") {
            await processFolderIncremental(file.id, file.name + "/", token, new Date(0).toISOString());
        } else {
            // Descargar y subir el archivo
            const blob = await downloadDriveFileREST(file.id, file.mimeType, token);
            await uploadBlobToGCS(BUCKET_NAME, file.name, blob, file.mimeType);
            console.log(`✅ Sincronizado en tiempo real: ${file.name}`);
        }

    } catch (error) {
        console.error('❌ Error procesando cambio en tiempo real:', error);

        // Reintentar después de un delay usando Pub/Sub para mejor escalabilidad
        await pubsub.topic(SYNC_TOPIC).publishMessage({
            data: Buffer.from(JSON.stringify({
                changeId: changeId,
                fileId: fileId,
                resourceState: resourceState,
                retryCount: 1
            }))
        });
    }
}

/**
 * Obtiene último page token de sincronización desde Firestore
 */
async function getLastSyncPageToken() {
    try {
        const doc = await firestore.collection(SYNC_COLLECTION).doc('page_token').get();
        if (doc.exists) {
            return doc.data().token;
        }

        // Si no existe, obtener uno nuevo
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });
        const client = await auth.getClient();
        const drive = google.drive({ version: 'v3', auth: client });
        const startPageToken = await drive.changes.getStartPageToken();

        await setLastSyncPageToken(startPageToken.data.startPageToken);
        return startPageToken.data.startPageToken;
    } catch (error) {
        console.error('Error obteniendo page token:', error);
        throw error;
    }
}

/**
 * Guarda último page token de sincronización en Firestore
 */
async function setLastSyncPageToken(token) {
    try {
        await firestore.collection(SYNC_COLLECTION).doc('page_token').set({
            token: token,
            updatedAt: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error guardando page token:', error);
        throw error;
    }
}

/**
 * Webhook mejorado para notificaciones en tiempo real de Drive
 */
app.post('/sync/webhook', async (req, res) => {
    console.log('📩 Notificación de Drive recibida!');

    // Verificar que es una notificación válida de Drive
    const resourceId = req.headers['x-goog-resource-id'];
    const resourceState = req.headers['x-goog-resource-state'];
    const resourceUri = req.headers['x-goog-resource-uri'];
    const channelId = req.headers['x-goog-channel-id'];

    if (!resourceId || !resourceState) {
        console.log('⚠️  Notificación inválida, faltan headers necesarios');
        return res.status(400).send('Notificación inválida');
    }

    // Generar ID único para este cambio
    const changeId = `${resourceId}-${Date.now()}`;

    // Responder inmediatamente (Drive requiere respuesta rápida)
    res.status(200).send('✅ Notificación recibida');

    // Procesar en segundo plano
    setTimeout(async () => {
        try {
            console.log(`🔄 Procesando cambio: ${resourceState} para resource: ${resourceId}`);

            // Para cambios, necesitamos obtener los archivos modificados
            if (resourceState === 'change' || resourceState === 'update' || resourceState === 'add') {
                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });

                const client = await auth.getClient();
                const drive = google.drive({ version: 'v3', auth: client });

                // Obtener el page token actual
                const pageToken = await getLastSyncPageToken();

                // Obtener los cambios recientes
                const changes = await drive.changes.list({
                    pageToken: pageToken,
                    pageSize: 10
                });

                // Procesar cada cambio
                for (const change of changes.data.changes) {
                    if (change.fileId) {
                        await processRealTimeChange(
                            `${change.fileId}-${Date.now()}`,
                            change.fileId,
                            resourceState
                        );
                    }
                }

                // Actualizar el page token
                if (changes.data.newStartPageToken) {
                    await setLastSyncPageToken(changes.data.newStartPageToken);
                }
            }
        } catch (error) {
            console.error('❌ Error procesando webhook:', error);
        }
    }, 1000);
});

/**
 * Ruta principal que Cloud Run health check requiere
 */
// Endpoint raíz para health checks - VERSIÓN MEJORADA
app.get('/', (req, res) => {
    const currentTime = new Date().toISOString();
    const formattedTime = new Date().toLocaleString('es-ES', {
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZone: 'America/Bogota'
    });

    const html = `
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TalentHub Sync Service</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            color: #333;
        }

        .container {
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            width: 90%;
            max-width: 1200px;
            overflow: hidden;
            margin: 20px;
        }

        .header {
            background: linear-gradient(135deg, #1a237e 0%, #283593 100%);
            color: white;
            padding: 40px;
            text-align: center;
            position: relative;
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #00bcd4, #4caf50);
        }

        .logo {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 15px;
            margin-bottom: 20px;
        }

        .logo-icon {
            font-size: 48px;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.1); }
            100% { transform: scale(1); }
        }

        h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            font-weight: 600;
        }

        .tagline {
            font-size: 1.2rem;
            opacity: 0.9;
            margin-bottom: 30px;
        }

        .status-badge {
            display: inline-block;
            background: #4caf50;
            color: white;
            padding: 8px 24px;
            border-radius: 50px;
            font-weight: 600;
            font-size: 1.1rem;
            box-shadow: 0 4px 15px rgba(76, 175, 80, 0.3);
        }

        .content {
            padding: 40px;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 30px;
            margin-bottom: 40px;
        }

        .card {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 25px;
            border-left: 4px solid #1a237e;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }

        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
        }

        .card h3 {
            color: #1a237e;
            margin-bottom: 15px;
            font-size: 1.3rem;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .card-icon {
            font-size: 24px;
        }

        .info-item {
            margin-bottom: 12px;
            padding-bottom: 12px;
            border-bottom: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .info-label {
            font-weight: 600;
            color: #555;
        }

        .info-value {
            color: #333;
            font-family: 'Courier New', monospace;
        }

        .endpoints {
            background: #1a237e;
            color: white;
            padding: 30px;
            border-radius: 15px;
            margin-top: 30px;
        }

        .endpoints h3 {
            margin-bottom: 20px;
            font-size: 1.4rem;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .endpoint-item {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 15px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background 0.3s ease;
        }

        .endpoint-item:hover {
            background: rgba(255, 255, 255, 0.2);
        }

        .method {
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.9rem;
        }

        .method.get { background: #4caf50; color: white; }
        .method.post { background: #ff9800; color: white; }
        .path { font-family: 'Courier New', monospace; }

        .footer {
            text-align: center;
            padding: 30px;
            color: #666;
            border-top: 1px solid #e0e0e0;
            margin-top: 40px;
        }

        .uptime {
            display: inline-block;
            background: #e3f2fd;
            color: #1a237e;
            padding: 10px 20px;
            border-radius: 10px;
            font-weight: 600;
            margin-top: 15px;
        }

        @media (max-width: 768px) {
            .container {
                margin: 10px;
                width: 95%;
            }
            
            .header {
                padding: 30px 20px;
            }
            
            .content {
                padding: 20px;
            }
            
            .grid {
                grid-template-columns: 1fr;
            }
            
            h1 {
                font-size: 2rem;
            }
        }
    </style>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">
                <div class="logo-icon">🔄</div>
                <div>
                    <h1>TalentHub Sync Service</h1>
                    <div class="tagline">Sincronización automatizada Drive → Google Cloud Storage</div>
                </div>
            </div>
            <div class="status-badge">
                <i class="fas fa-check-circle"></i> Servicio Activo
            </div>
        </div>
        
        <div class="content">
            <div class="grid">
                <div class="card">
                    <h3><i class="fas fa-info-circle card-icon"></i> Información del Servicio</h3>
                    <div class="info-item">
                        <span class="info-label">Estado:</span>
                        <span class="info-value" style="color: #4caf50; font-weight: 600;">
                            <i class="fas fa-check-circle"></i> Operacional
                        </span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Versión:</span>
                        <span class="info-value">1.0.0</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Última actualización:</span>
                        <span class="info-value">${formattedTime}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Entorno:</span>
                        <span class="info-value">${process.env.NODE_ENV || 'development'}</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3><i class="fas fa-database card-icon"></i> Configuración</h3>
                    <div class="info-item">
                        <span class="info-label">Proyecto GCP:</span>
                        <span class="info-value">${GOOGLE_CLOUD_PROJECT}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Bucket GCS:</span>
                        <span class="info-value">${BUCKET_NAME}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Carpeta Drive:</span>
                        <span class="info-value">${ROOT_FOLDER_ID}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Puerto:</span>
                        <span class="info-value">${PORT}</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3><i class="fas fa-chart-line card-icon"></i> Estadísticas</h3>
                    <div class="info-item">
                        <span class="info-label">Modo sincronización:</span>
                        <span class="info-value">${WEBHOOK_URL ? 'Webhook + Polling' : 'Polling'}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Intervalo polling:</span>
                        <span class="info-value">${POLLING_INTERVAL / 1000} segundos</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Sincronización:</span>
                        <span class="info-value">${WEBHOOK_URL ? '<span style="color:#4caf50;">Tiempo real</span>' : '<span style="color:#ff9800;">Programada</span>'}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Monitoreo:</span>
                        <span class="info-value"><span style="color:#4caf50;">Activo</span></span>
                    </div>
                </div>
            </div>
            
            <div class="endpoints">
                <h3><i class="fas fa-plug card-icon"></i> Endpoints Disponibles</h3>
                
                <div class="endpoint-item">
                    <div>
                        <span class="method get">GET</span>
                        <span class="path">/</span>
                    </div>
                    <span>Health Check (esta página)</span>
                </div>
                
                <div class="endpoint-item">
                    <div>
                        <span class="method post">POST</span>
                        <span class="path">/sync</span>
                    </div>
                    <span>Sincronización manual Drive → GCS</span>
                </div>
                
                <div class="endpoint-item">
                    <div>
                        <span class="method get">GET</span>
                        <span class="path">/debug/storage</span>
                    </div>
                    <span>Diagnóstico de Google Cloud Storage</span>
                </div>
                
                ${WEBHOOK_URL ? `
                <div class="endpoint-item">
                    <div>
                        <span class="method post">POST</span>
                        <span class="path">/sync/webhook</span>
                    </div>
                    <span>Webhook Drive (sincronización tiempo real)</span>
                </div>
                ` : ''}
                
                <div class="endpoint-item">
                    <div>
                        <span class="method get">GET</span>
                        <span class="path">/sync/scheduled</span>
                    </div>
                    <span>Health Check programado</span>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>TalentHub Sync Service © ${new Date().getFullYear()} - Sistema de sincronización automatizada</p>
            <p class="uptime">
                <i class="fas fa-clock"></i> Último check: ${new Date().toLocaleTimeString('es-ES')}
            </p>
            <p style="margin-top: 15px; font-size: 0.9rem;">
                <i class="fas fa-shield-alt"></i> Servicio seguro | 
                <i class="fas fa-bolt"></i> Alta disponibilidad | 
                <i class="fas fa-sync-alt"></i> Sincronización continua
            </p>
        </div>
    </div>
    
    <script>
        // Actualizar la hora automáticamente cada minuto
        function updateTime() {
            const now = new Date();
            const timeElement = document.querySelector('.uptime');
            if (timeElement) {
                timeElement.innerHTML = '<i class="fas fa-clock"></i> Último check: ' + 
                    now.toLocaleTimeString('es-ES', { 
                        hour: '2-digit', 
                        minute: '2-digit',
                        second: '2-digit'
                    });
            }
        }
        
        // Actualizar cada 60 segundos
        setInterval(updateTime, 60000);
        
        // Efecto de carga suave
        document.addEventListener('DOMContentLoaded', function() {
            const cards = document.querySelectorAll('.card');
            cards.forEach((card, index) => {
                card.style.opacity = '0';
                card.style.transform = 'translateY(20px)';
                setTimeout(() => {
                    card.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
                    card.style.opacity = '1';
                    card.style.transform = 'translateY(0)';
                }, index * 100);
            });
            
            // Efecto de ping para el estado
            const statusBadge = document.querySelector('.status-badge');
            if (statusBadge) {
                setInterval(() => {
                    statusBadge.style.transform = 'scale(1.05)';
                    setTimeout(() => {
                        statusBadge.style.transform = 'scale(1)';
                    }, 300);
                }, 3000);
            }
        });
    </script>
</body>
</html>
    `;

    res.status(200).send(html);
});

app.get('/debug/storage', async (req, res) => {
    try {
        if (!storage) {
            return res.status(500).json({ error: 'Storage no inicializado' });
        }

        // 1. Obtener información del proyecto
        const projectId = storage.projectId;
        const authClient = storage.authClient;

        // 2. Listar buckets
        const [buckets] = await storage.getBuckets();

        // 3. Verificar bucket específico
        const bucket = storage.bucket(BUCKET_NAME);
        const [bucketExists] = await bucket.exists();

        let bucketInfo = { exists: bucketExists };
        if (bucketExists) {
            const [metadata] = await bucket.getMetadata();
            const [files] = await bucket.getFiles({ maxResults: 5 });

            bucketInfo = {
                ...bucketInfo,
                metadata: {
                    name: metadata.name,
                    location: metadata.location,
                    storageClass: metadata.storageClass,
                    timeCreated: metadata.timeCreated
                },
                files: files.map(f => ({
                    name: f.name,
                    size: f.metadata.size,
                    contentType: f.metadata.contentType
                }))
            };
        }

        // 4. Verificar credenciales
        const credentialsInfo = {
            localFile: {
                path: LOCAL_CREDENTIALS_PATH,
                exists: fs.existsSync(LOCAL_CREDENTIALS_PATH)
            },
            env: {
                GOOGLE_APPLICATION_CREDENTIALS: process.env.GOOGLE_APPLICATION_CREDENTIALS,
                GOOGLE_CLOUD_PROJECT: process.env.GOOGLE_CLOUD_PROJECT
            }
        };

        if (fs.existsSync(LOCAL_CREDENTIALS_PATH)) {
            try {
                const keyContent = JSON.parse(fs.readFileSync(LOCAL_CREDENTIALS_PATH, 'utf8'));
                credentialsInfo.serviceAccount = {
                    clientEmail: keyContent.client_email,
                    projectId: keyContent.project_id,
                    privateKeyId: keyContent.private_key_id
                };
            } catch (e) {
                credentialsInfo.fileError = e.message;
            }
        }

        res.json({
            timestamp: new Date().toISOString(),
            project: projectId,
            storageInitialized: !!storage,
            credentials: credentialsInfo,
            buckets: buckets.map(b => b.name),
            targetBucket: bucketInfo
        });

    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack,
            code: error.code,
            details: error.details
        });
    }
});

/**
 * Ruta para ejecutar la sincronización manualmente
 */
app.post('/sync', async (req, res) => {
    console.log("🔍 Iniciando sincronización manual de Drive a GCS");

    try {
        const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
        const HAS_LOCAL_CREDS = require('fs').existsSync('./gcs-key.json');

        let auth;

        if (IS_LOCAL && HAS_LOCAL_CREDS) {
            console.log('🔑 Sincronización manual con credenciales locales');
            auth = new GoogleAuth({
                keyFile: './gcs-key.json',
                scopes: ['https://www.googleapis.com/auth/drive']
            });
        } else if (IS_LOCAL) {
            return res.status(400).json({
                status: 'error',
                message: 'No se encontraron credenciales locales. Crea un archivo gcs-key.json'
            });
        } else {
            auth = new GoogleAuth({
                scopes: ['https://www.googleapis.com/auth/drive']
            });
        }

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        const lastSyncTime = await getLastSyncTime();
        const currentTime = new Date().toISOString();

        console.log("Buscando archivos modificados desde: " + lastSyncTime);

        const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, lastSyncTime);
        await setLastSyncTime(currentTime);

        console.log(`✅ Sincronización manual completada. 
Archivos: ${stats.ok} 
Fallidos: ${stats.fail} 
Carpetas: ${stats.folders}`);

        res.status(200).json({
            status: 'success',
            message: 'Sincronización completada',
            stats: stats
        });

    } catch (error) {
        console.error("❌ Error en sincronización manual:", error.message);

        // Error específico para credenciales inválidas
        if (error.message.includes('invalid_grant')) {
            return res.status(401).json({
                status: 'error',
                message: 'Credenciales OAuth inválidas o expiradas. Regenera el archivo gcs-key.json'
            });
        }

        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

// async function getDriveTokenForPolling(isCloudRun) {
//     console.log('🔑 Obteniendo token de Drive...');

//     let auth;

//     if (isCloudRun) {
//         // 🔥 CLOUD RUN: Application Default Credentials CON SCOPES
//         auth = new GoogleAuth({
//             scopes: ['https://www.googleapis.com/auth/drive.readonly'],
//             projectId: GOOGLE_CLOUD_PROJECT
//         });

//         console.log('✅ Usando ADC en Cloud Run');
//     } else {
//         // 🔥 LOCAL: Archivo de credenciales
//         const CREDENTIALS_PATH = './gcs-key.json';

//         if (!fs.existsSync(CREDENTIALS_PATH)) {
//             throw new Error(`Archivo ${CREDENTIALS_PATH} no encontrado`);
//         }

//         auth = new GoogleAuth({
//             keyFile: CREDENTIALS_PATH,
//             scopes: ['https://www.googleapis.com/auth/drive.readonly']
//         });

//         console.log('✅ Usando credenciales locales');
//     }

//     const client = await auth.getClient();
//     const tokenResponse = await client.getAccessToken();

//     if (!tokenResponse?.token) {
//         throw new Error('No se pudo obtener token de acceso');
//     }

//     console.log(`✅ Token obtenido (${tokenResponse.token.length} caracteres)`);
//     return tokenResponse.token;
// }

async function runPollingCycle() {
    console.log(`🔄 Iniciando ciclo de polling...`);

    async function executePolling() {
        try {
            console.log('\n⏰ ========================================');
            console.log('⏰ CICLO DE POLLING');
            console.log('⏰ ========================================');

            const IS_CLOUD_RUN = process.env.K_SERVICE !== undefined;
            console.log(`📍 Entorno: ${IS_CLOUD_RUN ? 'Cloud Run' : 'Local'}`);

            // 🔥 OBTENER TOKEN - CORREGIDO
            let token;
            try {
                if (IS_CLOUD_RUN) {
                    // CLOUD RUN: Application Default Credentials
                    console.log('🔑 Usando ADC para Cloud Run');
                    const auth = new GoogleAuth({
                        scopes: ['https://www.googleapis.com/auth/drive.readonly'],
                        projectId: GOOGLE_CLOUD_PROJECT
                    });
                    const client = await auth.getClient();
                    const tokenResponse = await client.getAccessToken();
                    token = tokenResponse.token;
                } else {
                    // LOCAL: Archivo de credenciales
                    console.log('🔑 Usando credenciales locales');
                    if (!fs.existsSync(LOCAL_CREDENTIALS_PATH)) {
                        console.error('❌ Archivo gcs-key.json no encontrado');
                        setTimeout(executePolling, POLLING_INTERVAL);
                        return;
                    }
                    const auth = new GoogleAuth({
                        keyFile: LOCAL_CREDENTIALS_PATH,
                        scopes: ['https://www.googleapis.com/auth/drive.readonly']
                    });
                    const client = await auth.getClient();
                    const tokenResponse = await client.getAccessToken();
                    token = tokenResponse.token;
                }

                if (!token) {
                    throw new Error('No se pudo obtener token de acceso');
                }

                console.log(`✅ Token obtenido`);

            } catch (tokenError) {
                console.error(`❌ ERROR obteniendo token: ${tokenError.message}`);

                // Manejo específico de error 401
                if (tokenError.message.includes('401') || tokenError.message.includes('invalid_grant')) {
                    if (IS_CLOUD_RUN) {
                        console.error('🔐 ERROR 401 EN CLOUD RUN:');
                        console.error('   Verificar permisos de Drive para la Service Account');
                    } else {
                        console.error('🔐 ERROR 401 EN LOCAL:');
                        console.error('   Verificar que gcs-key.json sea válido');
                    }
                }

                // Reintentar en 2 minutos si es error 401
                setTimeout(executePolling, 120000);
                return;
            }

            // 🔥 OBTENER ÚLTIMA SINCRONIZACIÓN - CORREGIDO
            let lastRun;
            try {
                lastRun = await getLastSyncTime();

                // Validar que sea una fecha válida
                if (!lastRun || typeof lastRun !== 'string' || !Date.parse(lastRun)) {
                    console.warn('⚠️  lastSyncTime inválido, usando valor por defecto');
                    lastRun = '2000-01-01T00:00:00.000Z';
                }

                console.log(`📅 Última sincronización: ${lastRun}`);

            } catch (syncError) {
                console.error(`⚠️  Error obteniendo lastSyncTime: ${syncError.message}`);
                lastRun = '2000-01-01T00:00:00.000Z';
            }

            // 🔥 EJECUTAR SINCRONIZACIÓN
            console.log(`🔍 Buscando cambios desde: ${lastRun}`);
            const startTime = Date.now();

            const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, lastRun);

            const elapsedTime = Date.now() - startTime;

            // 🔥 ACTUALIZAR REGISTRO SI HUBO ÉXITOS
            if (stats.ok > 0) {
                const newSyncTime = new Date().toISOString();
                try {
                    await setLastSyncTime(newSyncTime);
                    console.log(`💾 Nuevo lastSyncTime guardado: ${newSyncTime}`);
                } catch (saveError) {
                    console.error(`⚠️  Error guardando lastSyncTime: ${saveError.message}`);
                }
            }

            // 🔥 MOSTRAR RESULTADOS
            console.log('\n📊 ========================================');
            console.log('📊 RESUMEN DEL CICLO');
            console.log('📊 ========================================');
            console.log(`✅ Archivos sincronizados: ${stats.ok}`);
            console.log(`❌ Archivos fallados: ${stats.fail}`);
            console.log(`📁 Carpetas procesadas: ${stats.folders}`);
            console.log(`⏱️  Tiempo total: ${(elapsedTime / 1000).toFixed(2)} segundos`);
            console.log(`📅 Finalizado: ${new Date().toLocaleTimeString()}`);

            // 🔥 PROGRAMAR SIGUIENTE CICLO
            console.log(`\n⏰ Próximo ciclo en ${POLLING_INTERVAL / 1000} segundos...`);
            setTimeout(executePolling, POLLING_INTERVAL);

        } catch (error) {
            console.error(`\n❌ ERROR en ciclo de polling: ${error.message}`);

            // Manejo específico de error 401
            if (error.message.includes('401')) {
                console.log('🔄 Error 401 detectado, reintentando en 2 minutos...');
                setTimeout(executePolling, 120000);
            } else {
                console.log(`🔄 Reintentando en ${POLLING_INTERVAL / 1000} segundos...`);
                setTimeout(executePolling, POLLING_INTERVAL);
            }
        }
    }

    // ✅ CORRECTO: Iniciar después de 5 segundos
    setTimeout(() => {
        executePolling().catch(err => {
            console.error('❌ Error fatal al iniciar polling:', err);
        });
    }, 5000);
}

app.get('/debug/auth', async (req, res) => {
    try {
        console.log('🔍 Ejecutando diagnóstico de autenticación...');

        const results = {
            timestamp: new Date().toISOString(),
            environment: process.env.K_SERVICE ? 'Cloud Run' : 'Local',
            projectId: GOOGLE_CLOUD_PROJECT,
            serviceAccount: null,
            authTest: null,
            driveTest: null
        };

        // 1. Verificar Service Account en Cloud Run
        if (process.env.K_SERVICE) {
            const auth = new GoogleAuth();
            const client = await auth.getClient();
            const credentials = await auth.getCredentials();

            results.serviceAccount = {
                projectId: credentials.projectId,
                client_email: credentials.client_email,
                token_expiry: credentials.res?.expiry_date ?
                    new Date(credentials.res.expiry_date).toISOString() : 'N/A'
            };

            // 2. Probar autenticación con Drive
            const token = await client.getAccessToken();
            results.authTest = {
                tokenObtained: !!token.token,
                tokenLength: token.token ? token.token.length : 0,
                success: true
            };

            // 3. Probar API de Drive
            try {
                const drive = google.drive({ version: 'v3', auth: client });
                const about = await drive.about.get({ fields: 'user' });
                results.driveTest = {
                    success: true,
                    user: about.data.user
                };
            } catch (driveError) {
                results.driveTest = {
                    success: false,
                    error: driveError.message,
                    code: driveError.code
                };
            }
        }

        res.json(results);

    } catch (error) {
        res.status(500).json({
            error: error.message,
            code: error.code,
            details: error.details
        });
    }
});

app.get('/debug/drive-access', async (req, res) => {
    try {
        console.log('🔍 Probando acceso a Drive API...');

        const IS_CLOUD_RUN = process.env.K_SERVICE !== undefined;
        const results = {
            timestamp: new Date().toISOString(),
            environment: IS_CLOUD_RUN ? 'Cloud Run' : 'Local',
            projectId: GOOGLE_CLOUD_PROJECT,
            testSteps: {}
        };

        // Paso 1: Obtener token
        let token;
        try {
            if (IS_CLOUD_RUN) {
                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
                    projectId: GOOGLE_CLOUD_PROJECT
                });
                const client = await auth.getClient();
                const tokenResponse = await client.getAccessToken();
                token = tokenResponse.token;
                results.testSteps.token = '✅ Obtenido (ADC)';
            } else {
                if (!fs.existsSync(LOCAL_CREDENTIALS_PATH)) {
                    results.testSteps.token = '❌ Archivo gcs-key.json no encontrado';
                    return res.json(results);
                }
                const auth = new GoogleAuth({
                    keyFile: LOCAL_CREDENTIALS_PATH,
                    scopes: ['https://www.googleapis.com/auth/drive.readonly']
                });
                const client = await auth.getClient();
                const tokenResponse = await client.getAccessToken();
                token = tokenResponse.token;
                results.testSteps.token = '✅ Obtenido (Archivo)';
            }
        } catch (tokenError) {
            results.testSteps.token = `❌ Error: ${tokenError.message}`;

            if (tokenError.message.includes('401')) {
                results.testSteps.suggestion = IS_CLOUD_RUN ?
                    'Verificar permisos de Drive para Service Account' :
                    'Verificar que gcs-key.json sea válido';
            }

            return res.json(results);
        }

        // Paso 2: Probar Drive API
        try {
            const response = await fetch(
                'https://www.googleapis.com/drive/v3/about?fields=user',
                {
                    headers: { Authorization: `Bearer ${token}` },
                    timeout: 10000
                }
            );

            if (response.ok) {
                const data = await response.json();
                results.testSteps.driveAccess = `✅ Concedido (Usuario: ${data.user.displayName || 'N/A'})`;
            } else if (response.status === 401) {
                results.testSteps.driveAccess = `❌ Error 401: Token inválido o expirado`;
            } else if (response.status === 403) {
                results.testSteps.driveAccess = `❌ Error 403: Sin permisos de Drive`;
                results.testSteps.suggestion = IS_CLOUD_RUN ?
                    'Ejecutar: gcloud projects add-iam-policy-binding [PROJECT] --member="serviceAccount:[SA-EMAIL]" --role="roles/drive.reader"' :
                    'Compartir carpetas con la cuenta de servicio del archivo gcs-key.json';
            } else {
                results.testSteps.driveAccess = `❌ Error ${response.status}: ${response.statusText}`;
            }
        } catch (apiError) {
            results.testSteps.driveAccess = `❌ Error API: ${apiError.message}`;
        }

        res.json(results);

    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack
        });
    }
});

app.get('/debug/full', async (req, res) => {
    try {
        const results = {
            timestamp: new Date().toISOString(),
            environment: process.env.K_SERVICE ? 'Cloud Run' : 'Local',
            projectId: GOOGLE_CLOUD_PROJECT,
            services: {},
            credentials: {},
            driveTest: null
        };

        // 1. Verificar servicios
        results.services.storage = !!storage;
        results.services.firestore = !!firestore;
        results.services.pubsub = !!pubsub;

        // 2. Verificar credenciales
        if (process.env.K_SERVICE) {
            results.credentials.mode = 'Application Default Credentials';

            try {
                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/cloud-platform']
                });
                const creds = await auth.getCredentials();
                results.credentials.serviceAccount = creds.client_email;
                results.credentials.projectId = creds.projectId;
            } catch (authError) {
                results.credentials.error = authError.message;
            }
        } else {
            results.credentials.mode = 'Local file';
            results.credentials.file = LOCAL_CREDENTIALS_PATH;
            results.credentials.exists = fs.existsSync(LOCAL_CREDENTIALS_PATH);

            if (results.credentials.exists) {
                try {
                    const keyContent = JSON.parse(fs.readFileSync(LOCAL_CREDENTIALS_PATH, 'utf8'));
                    results.credentials.serviceAccount = keyContent.client_email;
                } catch (e) {
                    results.credentials.error = e.message;
                }
            }
        }

        // 3. Probar Drive API
        try {
            const token = await getDriveTokenForPolling(!!process.env.K_SERVICE);

            const testResponse = await fetch(
                'https://www.googleapis.com/drive/v3/about?fields=user',
                { headers: { Authorization: `Bearer ${token}` } }
            );

            results.driveTest = {
                success: testResponse.ok,
                status: testResponse.status,
                statusText: testResponse.statusText
            };

        } catch (driveError) {
            results.driveTest = {
                success: false,
                error: driveError.message
            };
        }

        // 4. Verificar Firestore
        try {
            if (firestore) {
                const doc = await firestore.collection(SYNC_COLLECTION).doc('last_sync').get();
                results.firestore = {
                    connected: true,
                    lastSyncExists: doc.exists,
                    lastSyncTime: doc.exists ? doc.data().timestamp : 'N/A'
                };
            }
        } catch (firestoreError) {
            results.firestore = {
                connected: false,
                error: firestoreError.message,
                code: firestoreError.code
            };
        }

        res.json(results);

    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack
        });
    }
});

// Agrega este endpoint para los health checks de GooglegetLastSyncTime
app.post('/sync/scheduled', (req, res) => {
    console.log('✅ Health check recibido de Google');
    res.status(200).json({
        status: 'ok',
        message: 'Service is running',
        timestamp: new Date().toISOString()
    });
});

// También agrega un endpoint GET para health checks
app.get('/sync/scheduled', (req, res) => {
    console.log('✅ Health check GET recibido');
    res.status(200).json({
        status: 'ok',
        message: 'Service is healthy',
        timestamp: new Date().toISOString()
    });
});

/**
 * Inicia el polling periódico para LogySer Sync
 */
function startLogySerPolling() {
    const LOGYSER_POLLING_INTERVAL = 300000; // 5 minutos

    console.log(`\n🔄 Configurando polling automático de LogySer`);
    console.log(`   ⏰ Intervalo: ${LOGYSER_POLLING_INTERVAL / 1000} segundos`);

    async function executeLogySerCycle() {
        console.log('\n🔔 ========================================');
        console.log('🔔 CICLO AUTOMÁTICO DE LOGYSER');
        console.log('🔔 ========================================');
        console.log(`📅 Hora de inicio: ${new Date().toLocaleString()}`);

        try {
            // Verificar que LogySer esté inicializado
            if (!logyserSync.storage) {
                console.log('🔧 Inicializando LogySer...');
                await logyserSync.initialize();
            }

            // 🔥 CAMBIO IMPORTANTE: Usar sincronización COMPLETA (true) al menos la primera vez
            console.log('🔄 Ejecutando sincronización...');

            // Determinar si es primera ejecución o forzar completa
            let forceFullSync = false;

            // Verificar si ya se ha ejecutado antes (puedes usar un archivo o variable)
            const SYNC_STATE_FILE = 'logyser_last_full_sync.txt';
            const hasFullSyncedBefore = fs.existsSync(SYNC_STATE_FILE);

            if (!hasFullSyncedBefore) {
                console.log('🚀 ¡PRIMERA SINCRONIZACIÓN DETECTADA! Forzando sincronización completa...');
                forceFullSync = true;
                // Marcar que ya se hizo sincronización completa
                fs.writeFileSync(SYNC_STATE_FILE, new Date().toISOString());
            } else {
                console.log('🔄 Sincronización incremental (ya se hizo completa antes)');
                forceFullSync = false;
            }

            const results = await logyserSync.syncAll(forceFullSync);

            // Manejo seguro de resultados
            if (results && results.success !== false) {
                if (results.total) {
                    console.log(`📊 Resultado: ${results.total.success || 0} exitosos, ${results.total.failed || 0} fallidos`);
                } else if (results.success !== undefined) {
                    console.log(`📊 Resultado: ${results.success || 0} exitosos, ${results.failed || 0} fallidos`);
                }
            } else {
                console.log('📊 Sincronización completada (sin estadísticas)');
            }

        } catch (error) {
            console.error('❌ Error en ciclo LogySer:', error.message);
        } finally {
            // Programar próximo ciclo
            setTimeout(executeLogySerCycle, LOGYSER_POLLING_INTERVAL);
        }
    }

    // Iniciar después de 30 segundos
    setTimeout(executeLogySerCycle, 30000);
}

// ============ EJECUCIÓN AUTOMÁTICA DE LOGYSER ============
(async () => {
    console.log('\n🚀 INICIANDO EJECUCIÓN AUTOMÁTICA DE LOGYSER');
    console.log('============================================');

    try {
        // Dar un pequeño delay para que el servidor se inicialice primero
        setTimeout(async () => {
            console.log('🔧 Inicializando LogySer Sync...');
            await logyserSync.initialize();

            console.log('🔄 Ejecutando PRIMERA SINCRONIZACIÓN COMPLETA...');
            // 🔥 FORZAR SINCRONIZACIÓN COMPLETA LA PRIMERA VEZ
            const results = await logyserSync.syncAll(true);

            console.log('🎉 LogySer Sync completado inicialmente:');
            if (results && results.total) {
                console.log(`   ✅ Archivos exitosos: ${results.total.success || 0}`);
                console.log(`   ❌ Archivos fallidos: ${results.total.failed || 0}`);
                console.log(`   📁 Carpetas procesadas: ${results.total.folders || 0}`);
            } else if (results && results.success !== undefined) {
                console.log(`   ✅ Archivos exitosos: ${results.success || 0}`);
                console.log(`   ❌ Archivos fallidos: ${results.failed || 0}`);
            } else if (results && results.totalSuccess !== undefined) {
                console.log(`   ✅ Archivos exitosos: ${results.totalSuccess || 0}`);
                console.log(`   ❌ Archivos fallidos: ${results.totalFailed || 0}`);
            } else {
                console.log('   ⚠️  No se obtuvieron resultados detallados');
                if (results) {
                    console.log(`   🔍 Formato recibido: ${JSON.stringify(results).substring(0, 100)}...`);
                }
            }

            // Marcar que ya se hizo sincronización completa
            const SYNC_STATE_FILE = 'logyser_last_full_sync.txt';
            fs.writeFileSync(SYNC_STATE_FILE, new Date().toISOString());
            console.log('📝 Marcado que se realizó sincronización completa');

            // Iniciar polling periódico para LogySer
            startLogySerPolling();

        }, 5000); // Esperar 5 segundos después de iniciar el servidor

    } catch (error) {
        console.error('❌ Error en ejecución automática LogySer:', error.message);
        console.error('Detalles:', error);
    }
})();

app.listen(PORT, async () => {
    console.log(`🚀 Servidor ejecutándose en puerto ${PORT}`);
    console.log(`🌐 Ambiente: ${process.env.K_SERVICE ? 'Cloud Run' : 'Local'}`);

    try {
        // Inicializar servicios
        await initializeGoogleCloudServices();

        // VERIFICAR ESPECÍFICAMENTE PARA CLOUD RUN
        if (process.env.K_SERVICE) {
            console.log('🔧 Configuración específica para Cloud Run...');

            // Verificar variables críticas
            const criticalVars = ['GOOGLE_CLOUD_PROJECT', 'BUCKET_NAME', 'ROOT_FOLDER_ID'];
            criticalVars.forEach(varName => {
                console.log(`${varName}: ${process.env[varName] || 'NO CONFIGURADO'}`);
            });

            // Probar autenticación inmediatamente
            try {
                const auth = new GoogleAuth();
                const client = await auth.getClient();
                const token = await client.getAccessToken();

                if (token.token) {
                    console.log('✅ Autenticación ADC verificada en Cloud Run');
                    console.log(`   Token obtenido exitosamente`);
                }
            } catch (authError) {
                console.error('❌ ERROR DE AUTENTICACIÓN INICIAL:', authError.message);
                console.log('   Verifica que la Service Account tenga permisos de Drive');
            }
        }

        // Iniciar polling
        startDrivePolling();

    } catch (error) {
        console.error('❌ Error crítico durante inicialización:', error);
        process.exit(1); // Salir si hay error crítico
    }
});

module.exports = { app };