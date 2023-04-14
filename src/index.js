/* eslint-disable no-continue */
/* eslint-disable no-restricted-syntax */
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
import cliProgress from 'cli-progress';
import logger from './utils/logger.js';

dotenv.config();

const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);

const sourceURL = process.env.MONGO_SOURCE_URL;
const targetURL = process.env.MONGO_TARGET_URL;
const targetDB = 'monolith';
const dbsSourceToClone = ['accounts', 'providers', 'payments'];

let collecionsReady = 0;

async function cloneIndex(target, collection, indexKey, indexName) {
  await target.db(targetDB).collection(collection).createIndex(indexKey, {
    background: true,
    name: indexName
  });
}

async function cloneCollection(source, target, db, collection) {
  const sourceCollection = await source.db(db).collection(collection);
  const targetCollection = await target.db(targetDB).collection(collection);
  const allData = await sourceCollection.find().toArray();
  if (allData.length) await targetCollection.insertMany(allData);
}

async function main() {
  const clientTarget = new MongoClient(targetURL);
  const clientSource = new MongoClient(sourceURL);

  try {
    await clientTarget.connect();
    await clientSource.connect();

    const { databases: databasesSource } = await clientSource
      .db()
      .admin()
      .listDatabases();

    let collections = [];

    for await (const { name: dbName } of databasesSource) {
      if (dbsSourceToClone.includes(dbName)) {
        logger.info(`Fetch collections...`);
        let mapCollections = await clientSource
          .db(dbName)
          .listCollections()
          .toArray();

        mapCollections = mapCollections.map((collection) => {
          logger.info(`Found colletion ${collection.name}`);
          return {
            ...collection,
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
      }
    }

    logger.info(`Cloning  ${collections.length} collections`);

    bar.start(collections.length, 0);
    for await (const collection of collections) {
      await cloneCollection(
        clientSource,
        clientTarget,
        collection.dbName,
        collection.name
      );

      for await (const { key, name } of collection.indexes) {
        try {
          await cloneIndex(clientTarget, collection.name, key, name);
        } catch (error) {
          logger.error(
            `Error cloning index in collection ${collection.name}. Error: ${error}`
          );
        }
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
