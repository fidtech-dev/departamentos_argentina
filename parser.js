const fs = require('fs');
const {MongoClient} = require('mongodb');
const turf = require('@turf/turf');
const path = require('path');

const SOURCE_FILE = './source/departamentos-argentina.json';
const TARGET_COLLECTION_NAME = 'AR_GEOMETRY';
const TARGET_DATABASE_URI = 'mongodb://localhost:27017/AR_GEOMETRY';
const TARGET_DIR_PATH = './parsed';

async function parse() {
    console.log(`Reading file ${SOURCE_FILE}...`);
    const data = await fs.promises.readFile(SOURCE_FILE, {encoding: 'utf8'});
    // create an object with provinces in order to map departments and store the main province geometry
    console.log('Parsing data...');
    const locations = JSON.parse(data).features;
    const provinces = {};
    for (const location of locations) {
        const provinceName = location.properties.provincia;
        if (!provinces.hasOwnProperty(provinceName)) {
            provinces[provinceName] = {
                departments: [],
                geometry: null,
            };
        }
        const province = provinces[provinceName];
        province.departments.push({
            name: location.properties.departamento,
            geometry: location.geometry,
        });
    }

    // calculate the main province geometry using the departments polygons
    for (const key in provinces) {
        if (provinces.hasOwnProperty(key)) {
            console.log(`Calculating polygon for ${key}...`)
            const province = provinces[key];
            const provincePolygons = province.departments
                .filter((department) => department.geometry.coordinates[0].length >= 4)
                .map((department) => turf.polygon(department.geometry.coordinates));
            const provincePolygon = provincePolygons.reduce((lastPolygon, currentPolygon) => turf.union(lastPolygon, currentPolygon));
            province.geometry = provincePolygon.geometry;
        }
    }

    // write to database and disk
    console.log(`Writing to database and disk...`);
    const documents = [];
    for (const key in provinces) {
        if (provinces.hasOwnProperty(key)) {
            const provinceName = key;
            const province = provinces[provinceName];
            const document = {
                name: provinceName,
                geometry: province.geometry,
            };
            documents.push(document);
        }
    }
    // write to disk
    try {
        await fs.promises.access(TARGET_DIR_PATH)
    } catch (e) {
        if (e && e.code === 'ENOENT') {
            await fs.promises.mkdir(TARGET_DIR_PATH); // Create dir if it does not exist
        }
    }
    const diskPath = path.join(TARGET_DIR_PATH, 'AR_PROVINCES_GEOMETRY.json');
    await fs.promises.writeFile(diskPath, JSON.stringify(documents), {encoding: 'utf8'});
    console.log(`Data successfully written to disk: ${diskPath}`);
    // write to database
    console.log(`Connecting to database ${TARGET_DATABASE_URI}...`);
    const mongoClient = await MongoClient.connect(TARGET_DATABASE_URI, {useUnifiedTopology: true});
    console.log(`Creating collection ${TARGET_COLLECTION_NAME}...`);
    await mongoClient.db().collection(TARGET_COLLECTION_NAME).drop();
    await mongoClient.db().createCollection(TARGET_COLLECTION_NAME);
    await mongoClient.db().collection(TARGET_COLLECTION_NAME).insertMany(documents);
    console.log(`Data successfully written to database: ${TARGET_DATABASE_URI}, ${TARGET_COLLECTION_NAME}`);
}

console.log('Starting script...');
parse()
    .then(() => {
        console.log('Finished without errors.');
    }).catch((e) => {
    console.log(`An unexpected error occurred:`);
    console.log(e);
});

