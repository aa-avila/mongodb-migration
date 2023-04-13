/* eslint-disable no-continue */
/* eslint-disable no-restricted-syntax */
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
import cliProgress from 'cli-progress';
import logger from './utils/logger';

dotenv.config();

const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);

const targetURL = process.env.MONGO_TARGET_URL;
const sourceURL = process.env.MONGO_SOURCE_URL;

let collecionsReady = 0;

async function deleteDb(client, database) {
  await client.db(database).dropDatabase({ writeConcern: { w: 'majority' } });
}

async function cloneIndex(target, db, collection, indexKey, indexName) {
  await target.db(db).collection(collection).createIndex(indexKey, {
    background: true,
    name: indexName
  });
}

async function cloneCollection(source, target, db, collection) {
  const sourceCollection = await source.db(db).collection(collection);
  const targetCollection = await target.db(db).collection(collection);
  const allData = await sourceCollection.find().toArray();
  if (allData.length) await targetCollection.insertMany(allData);
}

async function main() {
  const clientTarget = new MongoClient(targetURL);
  const clientSource = new MongoClient(sourceURL);

  try {
    await clientTarget.connect();
    await clientSource.connect();

    const { databases } = await clientSource.db().admin().listDatabases();

    let collections = [];

    for await (const { name: dbName } of databases) {
      if (['accounts', 'providers', 'payments'].includes(dbName)) {
        logger.info(`Fetch collections...`);
        let mapCollections = await clientSource
          .db(dbName)
          .listCollections()
          .toArray();

        mapCollections = mapCollections.map((colletion) => {
          logger.info(`Found colletion ${colletion.name}`);
          return {
            ...colletion,
            dbName
          };
        });

        logger.info(`Fetch indexes...`);
        for await (const collection of mapCollections) {
          const indexes = await clientSource
            .db(dbName)
            .collection(collection.name)
            .listIndexes()
            .toArray();

          collection.indexes = indexes.filter((index) => index.name !== '_id_');

          logger.info(
            `Found ${collection.indexes.length} indexes in collection ${collection.name}`
          );
        }

        collections = [...collections, ...mapCollections];

        logger.info(`Deleting database: ${dbName}`);
        await deleteDb(clientTarget, dbName);
      }
    }
    logger.info(`Cloning  ${collections.length} collections`);

    bar.start(collections.length, 0);
    for await (const collection of collections) {
      // ignore collecions
      if (['applications', 'joborders', 'jobs'].includes(collection.name)) {
        collecionsReady += 1;
        continue;
      }

      await cloneCollection(
        clientSource,
        clientTarget,
        collection.dbName,
        collection.name
      );

      for await (const { key, name } of collection.indexes) {
        await cloneIndex(
          clientTarget,
          collection.dbName,
          collection.name,
          key,
          name
        );
      }
      collecionsReady += 1;
      bar.update(collecionsReady);
    }

    bar.stop();

    logger.info('Restoring the integrity of collections');

    logger.info('Finish');
  } catch (error) {
    logger.error(error);
  } finally {
    await clientSource.close();
    await clientTarget.close();
  }
}

main();
