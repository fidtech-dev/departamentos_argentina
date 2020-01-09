const fs = require('fs');
const {MongoClient} = require('mongodb');
const turf = require('@turf/turf');

const SOURCE_FILE = './source/departamentos-argentina.json';
const TARGET_COLLECTION_NAME = 'AR_GEOMETRY';
const TARGET_DATABASE_URI = 'mongodb://localhost:27017/AR_GEOMETRY';

async function parse() {
    console.log(`Reading file ${SOURCE_FILE}...`);
    const data = await fs.promises.readFile(SOURCE_FILE, {encoding: 'utf8'});
    // create an object with provinces in order to map departments and store the union polygon
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

    // insert into the database
    console.log(`Connecting to database ${TARGET_DATABASE_URI}...`);
    const mongoClient = await MongoClient.connect(TARGET_DATABASE_URI, {useUnifiedTopology: true});
    try {
        console.log(`Creating collection ${TARGET_COLLECTION_NAME}...`);
        await mongoClient.db().collection(TARGET_COLLECTION_NAME).drop();
        await mongoClient.db().createCollection(TARGET_COLLECTION_NAME);
        for (const key in provinces) {
                if (provinces.hasOwnProperty(key)) {
                    const provinceName = key;
                    const province = provinces[provinceName];
                    console.log(`Inserting province ${provinceName}...`);
                    await mongoClient.db().collection(TARGET_COLLECTION_NAME).insertOne({
                        name: provinceName,
                        geometry: province.geometry,
                    });
                }
            }
            console.log(`Data parsed and written to ${TARGET_DATABASE_URI}, ${TARGET_COLLECTION_NAME}`);
        } catch (e) {
            throw e;
        }
}

console.log('Starting script...');
parse()
    .then(() => {
        console.log('Finished without errors.');
    }).catch((e) => {
        console.log(`An unexpected error occurred:`);
        console.log(e);
    });

