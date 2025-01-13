import { randomBytes } from 'crypto'
import NodeCache from 'node-cache'
import { DEFAULT_CACHE_TTLS } from '../Defaults'
import type { AuthenticationCreds, CacheStore, SignalDataSet, SignalDataTypeMap, SignalKeyStore, SignalKeyStoreWithTransaction, TransactionCapabilityOptions } from '../Types'
import { Curve, signedKeyPair } from './crypto'
import { delay, generateRegistrationId } from './generics'
import { ILogger } from './logger'

/**
 * Adds caching capability to a SignalKeyStore
 * @param store the store to add caching to
 * @param logger to log trace events
 * @param _cache cache store to use
 */
export function makeCacheableSignalKeyStore(
	store: SignalKeyStore,
	logger: ILogger,
	_cache?: CacheStore
): SignalKeyStore {
	const cache = _cache || new NodeCache({
		stdTTL: DEFAULT_CACHE_TTLS.SIGNAL_STORE, // 5 minutes
		useClones: false,
		deleteOnExpire: true,
	})

	function getUniqueId(type: string, id: string) {
		return `${type}.${id}`
	}

	return {
		async get(type, ids) {
			const data: { [_: string]: SignalDataTypeMap[typeof type] } = { }
			const idsToFetch: string[] = []
			for(const id of ids) {
				const item = cache.get<SignalDataTypeMap[typeof type]>(getUniqueId(type, id))
				if(typeof item !== 'undefined') {
					data[id] = item
				} else {
					idsToFetch.push(id)
				}
			}

			if(idsToFetch.length) {
				logger.trace({ items: idsToFetch.length }, 'loading from store')
				const fetched = await store.get(type, idsToFetch)
				for(const id of idsToFetch) {
					const item = fetched[id]
					if(item) {
						data[id] = item
						cache.set(getUniqueId(type, id), item)
					}
				}
			}

			return data
		},
		async set(data) {
			let keys = 0
			for(const type in data) {
				for(const id in data[type]) {
					cache.set(getUniqueId(type, id), data[type][id])
					keys += 1
				}
			}

			logger.trace({ keys }, 'updated cache')

			await store.set(data)
		},
		async clear() {
			cache.flushAll()
			await store.clear?.()
		}
	}
}

/**
 * Adds DB like transaction capability (https://en.wikipedia.org/wiki/Database_transaction) to the SignalKeyStore,
 * this allows batch read & write operations & improves the performance of the lib
 * @param state the key store to apply this capability to
 * @param logger logger to log events
 * @returns SignalKeyStore with transaction capability
 */
export const addTransactionCapability = (
	state: SignalKeyStore,
	logger: ILogger,
	{ maxCommitRetries, delayBetweenTriesMs }: TransactionCapabilityOptions
): SignalKeyStoreWithTransaction => {
	// number of queries made to the DB during the transaction
	// only there for logging purposes
	let dbQueriesInTransaction = 0
	let transactionCache: SignalDataSet = { }
	let mutations: SignalDataSet = { }

	let transactionsInProgress = 0

	return {
		get: async(type, ids) => {
			if(isInTransaction()) {
				const dict = transactionCache[type]
				const idsRequiringFetch = dict
					? ids.filter(item => typeof dict[item] === 'undefined')
					: ids
				// only fetch if there are any items to fetch
				if(idsRequiringFetch.length) {
					dbQueriesInTransaction += 1
					const result = await state.get(type, idsRequiringFetch)

					transactionCache[type] ||= {}
					Object.assign(
						transactionCache[type]!,
						result
					)
				}

				return ids.reduce(
					(dict, id) => {
						const value = transactionCache[type]?.[id]
						if(value) {
							dict[id] = value
						}

						return dict
					}, { }
				)
			} else {
				return state.get(type, ids)
			}
		},
		set: data => {
			if(isInTransaction()) {
				logger.trace({ types: Object.keys(data) }, 'caching in transaction')
				for(const key in data) {
					transactionCache[key] = transactionCache[key] || { }
					Object.assign(transactionCache[key], data[key])

					mutations[key] = mutations[key] || { }
					Object.assign(mutations[key], data[key])
				}
			} else {
				return state.set(data)
			}
		},
		isInTransaction,
		async transaction(work) {
			let result: Awaited<ReturnType<typeof work>>
			transactionsInProgress += 1
			if(transactionsInProgress === 1) {
				logger.trace('entering transaction')
			}

			try {
				result = await work()
				// commit if this is the outermost transaction
				if(transactionsInProgress === 1) {
					if(Object.keys(mutations).length) {
						logger.trace('committing transaction')
						// retry mechanism to ensure we've some recovery
						// in case a transaction fails in the first attempt
						let tries = maxCommitRetries
						while(tries) {
							tries -= 1
							//eslint-disable-next-line max-depth
							try {
								await state.set(mutations)
								logger.trace({ dbQueriesInTransaction }, 'committed transaction')
								break
							} catch(error) {
								logger.warn(`failed to commit ${Object.keys(mutations).length} mutations, tries left=${tries}`)
								await delay(delayBetweenTriesMs)
							}
						}
					} else {
						logger.trace('no mutations in transaction')
					}
				}
			} finally {
				transactionsInProgress -= 1
				if(transactionsInProgress === 0) {
					transactionCache = { }
					mutations = { }
					dbQueriesInTransaction = 0
				}
			}

			return result
		}
	}

	function isInTransaction() {
		return transactionsInProgress > 0
	}
}

export const initAuthCreds = (): AuthenticationCreds => {
	const identityKey = Curve.generateKeyPair()
	return {
		noiseKey: Curve.generateKeyPair(),
		pairingEphemeralKeyPair: Curve.generateKeyPair(),
		signedIdentityKey: identityKey,
		signedPreKey: signedKeyPair(identityKey, 1),
		registrationId: generateRegistrationId(),
		advSecretKey: randomBytes(32).toString('base64'),
		processedHistoryMessages: [],
		nextPreKeyId: 1,
		firstUnuploadedPreKeyId: 1,
		accountSyncCounter: 0,
		accountSettings: {
			unarchiveChats: false
		},
		registered: false,
		pairingCode: undefined,
		lastPropHash: undefined,
		routingInfo: undefined,
	}
}
