import Promise from 'bluebird';
import {EventStorePartition as Partition} from './EventStorePartition';
import {InMemoryPersistenceStore} from './persistence/inmemory/InMemoryPersistenceStore';
import { DEFAULT_PARTITION_ID } from './persistence';

const _PENDING = {};

export class EventStore {
	constructor(persistenceStore, dispatchService) {
		this._store = persistenceStore || new InMemoryPersistenceStore();
		this._dispatchService = dispatchService;
		this._partitions = {};
	}

	openPartition(partitionId, callback) {
		let partition = null;
		partitionId = partitionId || DEFAULT_PARTITION_ID;
		partition = this._partitions[partitionId];
		if(!partition) {
			this._partitions[partitionId] = _PENDING;
			return this._store.openPartition(partitionId).then((persistencePartition) => {
				partition = new Partition(partitionId, persistencePartition, this._dispatchService);
				return this._partitions[partitionId] = partition;
			}).nodeify(callback);
		} else {
			//_PENDING is set when the partition is loading, wait 5ms at a time to see if it has been loaded
			if(this._partitions[partitionId] === _PENDING) {
				const promise = new Promise((resolve) => {
					const resolver = () => {
						const partition = this._partitions[partitionId];
						if(partition === _PENDING) {
							setTimeout(resolver, 5);
						} else {
							resolve(partition);
						}
					}
					resolver();
				});
				return promise.nodeify(callback);
			} else {
				return Promise.resolve(this._partitions[partitionId]).nodeify(callback);
			}
		}
	}
}
