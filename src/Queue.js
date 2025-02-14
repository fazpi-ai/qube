import { fileURLToPath } from 'url';
import { dirname, resolve, join } from 'path';
import fs from 'fs';
import Redis from 'ioredis';
import { createPool } from 'generic-pool';
import pino from 'pino';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export default class Queue {
    pendingGroupConsumers = [];
    processMap = new Map();
    isReady = false;
    scripts = {};

    inactivityTimeout = 2000;

    constructor(credentials, logLevel = 'debug') {
        this.credentials = credentials;
        this.logger = pino({ level: logLevel });
        this.client = new Redis(credentials);
        this.client.on('connect', () => this.logger.debug('Conexión a Redis establecida'));
        this.client.on('error', (err) => this.logger.error(`Error en Redis: ${err.message}`));

        this.pool = createPool({
            create: async () => {
                const client = new Redis(credentials);
                client.on('error', (err) => this.logger.error(`Error en conexión del pool: ${err.message}`));
                return client;
            },
            destroy: async (client) => {
                this.logger.debug('Cerrando cliente de Redis del pool');
                await client.quit();
            },
            validate: async (client) => {
                try {
                    await client.ping();
                    return true;
                } catch {
                    return false;
                }
            }
        }, { max: 1000, min: 2 });

        this.logger.debug('Pool de conexiones creado');
        this.subscriber = new Redis(this.credentials);
        this.publisher = new Redis(this.credentials);

        this.instanceId = Math.random().toString(36).substr(2, 9);
        this.localTimers = new Map();
    }

    async init() {
        this.enqueueScript = fs.readFileSync(join(__dirname, './lua-scripts/enqueue.lua'), 'utf8');
        this.dequeueScript = fs.readFileSync(join(__dirname, './lua-scripts/dequeue.lua'), 'utf8');
        this.updateStatusScript = fs.readFileSync(join(__dirname, './lua-scripts/update_status.lua'), 'utf8');
        this.getStatusScript = fs.readFileSync(join(__dirname, './lua-scripts/get_status.lua'), 'utf8');

        this.scriptsLoaded = (async () => {
            const client = await this.pool.acquire();
            try {
                this.enqueueSha = await client.script('LOAD', this.enqueueScript);
                this.dequeueSha = await client.script('LOAD', this.dequeueScript);
                this.updateStatusSha = await client.script('LOAD', this.updateStatusScript);
                this.getStatusSha = await client.script('LOAD', this.getStatusScript);
            } finally {
                await this.pool.release(client);
            }
        })();
        await this.scriptsLoaded;
        this.logger.info("✅ Scripts Lua cargados correctamente.");
        this.listenToPubSub();
    }

    async getClient() {
        this.logger.debug('Adquiriendo cliente de Redis');
        this.getPoolStats()
        return this.pool.acquire();
    }

    async releaseClient(client) {
        this.logger.debug('Liberando cliente de Redis');
        this.getPoolStats()
        await this.pool.release(client);
    }

    async runLuaScript(scriptName, keys = [], args = []) {
        if (!this.scripts[scriptName]) {
            throw new Error(`Script Lua no encontrado: ${scriptName}`);
        }
        const client = await this.getClient();
        try {
            return await client.eval(this.scripts[scriptName], keys.length, ...keys, ...args);
        } catch (err) {
            this.logger.error(`❌ Error ejecutando script Lua '${scriptName}': ${err.message}`);
            throw err;
        } finally {
            await this.releaseClient(client);
        }
    }

    // Agrega este método a la clase Queue:
    getPoolStats() {
        this.logger.info(`👾 Pool Stats - Size: ${this.pool.size}, Available: ${this.pool.available}, Borrowed: ${this.pool.borrowed}, Pending: ${this.pool.pending}`);
    }

    async safeEvalsha(scriptKey, keysCount, ...args) {
        const client = await this.getClient();
        try {
            return await client.evalsha(this[`${scriptKey}Sha`], keysCount, ...args);
        } catch (err) {
            if (err.message.includes('NOSCRIPT')) {
                // Reload the script if it was not found
                this[`${scriptKey}Sha`] = await client.script('LOAD', this[`${scriptKey}Script`]);
                return await client.evalsha(this[`${scriptKey}Sha`], keysCount, ...args);
            }
            throw err;
        } finally {
            await this.releaseClient(client);
        }
    }

    async updateJobStatus(jobId, newStatus) {
        await this.scriptsLoaded;
        await this.safeEvalsha('updateStatus', 0, jobId, newStatus);
    }

    async listenToPubSub() {
        this.logger.info("📡 Suscribiéndose al canal 'QUEUE:NEWJOB' en Redis Pub/Sub...");
        await new Promise((resolve, reject) => {
            this.subscriber.subscribe('QUEUE:NEWJOB', (err, count) => {
                if (err) {
                    this.logger.error(`❌ Error suscribiéndose a 'QUEUE:NEWJOB': ${err.message}`);
                    reject(err);
                } else {
                    this.logger.info(`✅ Suscrito a ${count} canal(es), esperando mensajes...`);
                    this.isReady = true;
                    resolve();
                }
            });
        });
        this.subscriber.on('message', async (channel, message) => {
            if (channel === 'QUEUE:NEWJOB') {
                const { queueName, groupName } = JSON.parse(message);
                this.logger.info(`🔔 Notificación recibida: Nuevo mensaje en queueName '${queueName}', grupo '${groupName}'`);
                if (this.processMap.has(queueName)) {
                    this.logger.info(`✅ Se encontró callback para el queueName '${queueName}', ejecutando...`);
                    await this.startGroupConsumer(queueName, groupName, null, false, this.processMap.get(queueName).nConsumers);
                } else {
                    this.logger.warn(`⚠️ No hay callback asociado al queueName '${queueName}', ignorando notificación.`);
                }
            }
        });
    }

    async addActiveConsumer(queueName, groupName, workerId, info) {
        const consumerKey = `qube:${queueName}:${groupName}:${workerId}`;
        const client = await this.getClient();
        await client.hset('activeGroupConsumers', consumerKey, JSON.stringify(info));
        await this.releaseClient(client);
    }

    async getActiveConsumer(queueName, groupName, workerId) {
        const consumerKey = `qube:${queueName}:${groupName}:${workerId}`;
        const client = await this.getClient();
        const data = await client.hget('activeGroupConsumers', consumerKey);
        await this.releaseClient(client);
        return data ? JSON.parse(data) : null;
    }

    async deleteActiveConsumer(queueName, groupName, workerId) {
        const consumerKey = `qube:${queueName}:${groupName}:${workerId}`;
        const client = await this.getClient();
        await client.hdel('activeGroupConsumers', consumerKey);
        await this.releaseClient(client);
    }

    async countActiveConsumersForGroup(queueName, groupName) {
        const client = await this.getClient();
        const keys = await client.hkeys('activeGroupConsumers');
        const prefix = `qube:${queueName}:${groupName}:`;
        const count = keys.filter(key => key.startsWith(prefix)).length;
        await this.releaseClient(client);
        return count;
    }

    async checkGroupConsumerInactivity(queueName, groupName, workerId) {
        const consumerInfo = await this.getActiveConsumer(queueName, groupName, workerId);
        return consumerInfo && consumerInfo.shouldStop;
    }

    async stopGroupConsumer(queueName, groupName, workerId) {
        const consumerKey = `qube:${queueName}:${groupName}:${workerId}`;
        const consumerInfo = await this.getActiveConsumer(queueName, groupName, workerId);
        if (consumerInfo) {
            if (consumerInfo.owner === this.instanceId && this.localTimers.has(consumerKey)) {
                clearTimeout(this.localTimers.get(consumerKey));
                this.localTimers.delete(consumerKey);
            }
            await this.deleteActiveConsumer(queueName, groupName, workerId);
            await this.processPendingConsumers(this.processMap.get(queueName).nConsumers);
        }
    }

    async processPendingConsumers(nConsumers) {
        while (this.pendingGroupConsumers.length > 0) {
            const { queueName, groupName, groupKey } = this.pendingGroupConsumers.shift();
            const count = await this.countActiveConsumersForGroup(queueName, groupName);
            if (count < nConsumers) {
                await this.startGroupConsumer(queueName, groupName, groupKey, true, nConsumers);
            }
        }
    }

    async updateProgress(jobId, value) {
        const client = await this.getClient();
        try {
            await client.hset(`qube:job:${jobId}`, 'progress', value);
        } finally {
            await this.releaseClient(client);
        }
    }

    async resetGroupConsumerTimer(queueName, groupName, workerId) {
        const consumerKey = `qube:${queueName}:${groupName}:${workerId}`;
        const consumerInfo = await this.getActiveConsumer(queueName, groupName, workerId);
        if (consumerInfo && consumerInfo.owner === this.instanceId) {
            if (this.localTimers.has(consumerKey)) {
                clearTimeout(this.localTimers.get(consumerKey));
            }
            const timer = setTimeout(async () => {
                const current = await this.getActiveConsumer(queueName, groupName, workerId);
                if (current && !current.shouldStop) {
                    current.shouldStop = true;
                    await this.addActiveConsumer(queueName, groupName, workerId, current);
                }
            }, this.inactivityTimeout);
            this.localTimers.set(consumerKey, timer);
        }
    }

    async groupWorker(queueName, groupName, groupKey, workerId) {
        await this.scriptsLoaded;
        const client = await this.getClient();
        try {
            this.logger.info("GROUP WORKER:", { queueName, groupName, groupKey, workerId });
            while (true) {
                // const result = await client.evalsha(this.dequeueSha, 1, groupKey);
                const result = await this.safeEvalsha('dequeue', 1, groupKey);
                this.logger.debug("result:", result);
                if (result) {
                    await this.resetGroupConsumerTimer(queueName, groupName, workerId);
                    const [jobId, jobDataRaw, groupNameFromJob] = result;
                    const jobDataParsed = jobDataRaw ? JSON.parse(jobDataRaw) : null;
                    const job = {
                        id: jobId,
                        data: jobDataParsed,
                        groupName: groupNameFromJob ? groupNameFromJob.toString() : undefined,
                        progress: (value) => this.updateProgress(jobId, value)
                    };
                    await this.processJob(queueName, job, this.processMap.get(queueName).callback);
                } else {
                    const shouldStop = await this.checkGroupConsumerInactivity(queueName, groupName, workerId);
                    if (shouldStop) {
                        await this.stopGroupConsumer(queueName, groupName, workerId);
                        break;
                    } else {
                        await new Promise((r) => setTimeout(r, 1000));
                    }
                }
            }
        } finally {
            await this.releaseClient(client);
        }
    }

    async processJob(queueName, job, fn) {
        const done = async (err) => {
            if (err) {
                await this.updateJobStatus(job.id, 'failed');
            } else {
                await this.updateJobStatus(job.id, 'completed');
            }
        };
        try {
            await fn(job, done);
        } catch (error) {
            await this.updateJobStatus(job.id, 'failed');
        }
    }

    async startGroupConsumer(queueName, groupName, groupKey = null, fromPending = false, nConsumers = 1) {
        if (!groupKey) {
            groupKey = `qube:${queueName}:group:${groupName}`;
        }
        const count = await this.countActiveConsumersForGroup(queueName, groupName);
        if (count >= nConsumers) {
            if (!fromPending) {
                this.pendingGroupConsumers.push({ queueName, groupName, groupKey });
            }
            return;
        }
        const workerId = Math.random().toString(36).substr(2, 9);
        await this.addActiveConsumer(queueName, groupName, workerId, { owner: this.instanceId, shouldStop: false, workerId });
        const timer = setTimeout(async () => {
            const current = await this.getActiveConsumer(queueName, groupName, workerId);
            if (current && !current.shouldStop) {
                current.shouldStop = true;
                await this.addActiveConsumer(queueName, groupName, workerId, current);
            }
        }, this.inactivityTimeout);
        const consumerKey = `qube:${queueName}:${groupName}:${workerId}`;
        this.localTimers.set(consumerKey, timer);
        this.groupWorker(queueName, groupName, groupKey, workerId);
    }

    async add(queueName, groupName, data) {
        await this.scriptsLoaded;
        const client = await this.getClient();
        try {
            const queueKey = `qube:${queueName}:groups`;
            const groupKey = `qube:${queueName}:group:${groupName}`;
            const jobId = await this.safeEvalsha('enqueue', 2, queueKey, groupKey, JSON.stringify(data), groupName);
            /*
            const jobId = await client.evalsha(
                this.enqueueSha,
                2,
                queueKey,
                groupKey,
                JSON.stringify(data),
                groupName
            );
            */
            await this.publisher.publish('QUEUE:NEWJOB', JSON.stringify({ queueName, groupName }));
            return jobId;
        } finally {
            await this.releaseClient(client);
        }
    }

    async process(queueName, nConsumers = 1, callback) {
        await this.scriptsLoaded;
        const client = await this.getClient();
        try {
            this.processMap.set(queueName, { callback, nConsumers });
            const groups = await client.smembers(`qube:${queueName}:groups`);
            for (let groupName of groups) {
                if (groupName.startsWith(`qube:${queueName}:group:`)) {
                    groupName = groupName.split(`qube:${queueName}:group:`)[1];
                }
                for (let i = 0; i < nConsumers; i++) {
                    await this.startGroupConsumer(queueName, groupName, null, false, nConsumers);
                }
            }
        } finally {
            await this.releaseClient(client);
        }
    }

    async close() {
        this.logger.debug('Cerrando conexiones de Redis y pool');
        await this.pool.drain();
        await this.pool.clear();
        await this.client.quit();
    }
}