import { Promise as BluebirdPromise } from 'bluebird';

const changedSnapshotStoredTime = (indexDb: IDBFactory, dbName: string, streamId: string, newDate: string) => {
  return new BluebirdPromise((resolve, reject) => {
    const openRequest = indexDb.open('tw_' + dbName + '_master');

    openRequest.onsuccess = (event) => {
      if (!event.target) {
        return;
      }

      // @ts-ignore
      const db: IDBDatabase = event.target.result;
      const transaction = db.transaction(['snapshots'], 'readwrite');
      const snapshotStore = transaction.objectStore('snapshots');
      const cursorRequest = snapshotStore.openCursor();

      cursorRequest.onsuccess = (e) => {
        // @ts-ignore
        const result = e.target._result;

        if (result) {
          // @ts-ignore
          const snapshot = e.target._result.value;
          if (snapshot.id === streamId) {
            const updateRequest = result.update({ ...snapshot, storedDateTime: newDate });
            updateRequest.onsuccess = () => {
              resolve();
            };

            updateRequest.onerror = () => {
              reject();
            };
          }

          result.continue();
        }
      };
    };
  });
};

export { changedSnapshotStoredTime };
