const util = require('util')
const fs = require('fs-extra')
const pump = util.promisify(require('pump'))
const path = require('path')
const { Writable } = require('stream')
const JSONStream = require('JSONStream')
const zlib = require('zlib')
const pointOnFeature = require('@turf/point-on-feature')

const datasetSchema = [
  {
    key: 'code',
    title: 'Code parcelle',
    type: 'string',
    'x-refersTo': 'http://dbpedia.org/ontology/codeLandRegistry',
    'x-capabilities': {
      values: false,
      textStandard: false,
      text: false,
      textAgg: false,
      insensitive: false
    }
  },
  {
    key: 'coord',
    title: 'Coordonnées parcelle',
    type: 'string',
    'x-refersTo': 'http://www.w3.org/2003/01/geo/wgs84_pos#lat_long',
    'x-capabilities': {
      index: false,
      values: false,
      textStandard: false,
      text: false,
      textAgg: false,
      insensitive: false,
      geoShape: false
    }
  }
]

const fetch = async (axios, log, date, dep, tmpDir) => {
  const tmpFile = path.join(tmpDir, `${date}-${dep}.json.gz`)

  // this is used only in dev
  if (await fs.pathExists(tmpFile)) {
    await log.info('Le fichier existe déjà')
    return tmpFile
  }

  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(tmpFile)

  const url = `https://cadastre.data.gouv.fr/data/etalab-cadastre/${date}/geojson/departements/${dep}/cadastre-${dep}-parcelles.json.gz`
  await log.info('Télécharge le fichier ' + url)
  const res = await axios.get(url, { responseType: 'stream' })
  await pump(res.data, fs.createWriteStream(tmpFile))

  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(tmpFile, 'r')
  await fs.fsync(fd)
  await fs.close(fd)

  return tmpFile
}

let _stopped

exports.run = async ({ processingConfig, processingId, tmpDir, axios, log, patchConfig }) => {
  let dataset
  if (processingConfig.datasetMode === 'create') {
    await log.step('Création du jeu de données')
    const body = {
      title: processingConfig.dataset.title,
      isRest: true,
      schema: datasetSchema,
      masterData: {
        bulkSearchs: [{
          id: 'parcelle-coords',
          title: 'Récupérer les positions de parcelles à partir de leurs codes.',
          description: '',
          input: [{
            type: 'equals',
            property: {
              key: 'code',
              title: 'Code parcelle',
              type: 'string',
              'x-refersTo': 'http://dbpedia.org/ontology/codeLandRegistry'
            }
          }]
        }]
      },
      extras: { processingId }
    }
    if (processingConfig.dataset.id) {
      try {
        await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)
        throw new Error('le jeu de données existe déjà')
      } catch (err) {
        if (err.status !== 404) throw err
      }
      dataset = (await axios.put('api/v1/datasets/' + processingConfig.dataset.id, body)).data
    } else {
      dataset = (await axios.post('api/v1/datasets', body)).data
    }
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  } else if (processingConfig.datasetMode === 'update') {
    await log.step('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  await log.step('Vérification des dates de publication')
  const datesHtml = (await axios.get('https://cadastre.data.gouv.fr/data/etalab-cadastre/')).data
  let dates = [...new Set([...datesHtml.matchAll(/[0-9]{4}-[0-9]{2}-[0-9]{2}/g)].map(m => m[0]))].sort()
  await log.info('dates de publications : ' + dates.join(', '))
  if (processingConfig.lastDate) {
    await log.info('date de la dernière publication traitée : ' + processingConfig.lastDate)
    dates = dates.filter(d => d > processingConfig.lastDate)
  } else {
    await log.info('aucune publication traitée précédemment, traite toutes les dates')
  }

  for (const dep of processingConfig.deps) {
    await log.step(`traitement du département ${dep}`)
    const coords = {}
    for (const date of dates) {
      if (_stopped) return await log.info('interruption demandée')
      let tmpFile
      try {
        tmpFile = await fetch(axios, log, date, dep, tmpDir)
      } catch (err) {
        if (err.status === 404) {
          await log.info('Pas de fichier trouvé')
          continue
        }
        throw err
      }
      await pump(
        fs.createReadStream(tmpFile),
        zlib.createUnzip(),
        JSONStream.parse('features.*'), new Writable({
          objectMode: true,
          write (parcelle, encoding, callback) {
            const point = pointOnFeature(parcelle)
            coords[parcelle.properties.id] = `${point.geometry.coordinates[1]},${point.geometry.coordinates[0]}`
            callback()
          }
        })
      )
    }
    const bulk = Object.keys(coords).map(code => ({
      _id: code,
      code,
      coord: coords[code]
    }))
    await log.info(`envoi de ${bulk.length} lignes vers le jeu de données`)
    while (bulk.length) {
      if (_stopped) return await log.info('interruption demandée')
      const lines = bulk.splice(0, 1000)
      const res = await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
      if (res.data.nbErrors) {
        log.error(`${res.data.nbErrors} échecs sur ${lines.length} lignes à insérer`, res.data.errors)
        throw new Error('échec à l\'insertion des lignes dans le jeu de données')
      }
    }
  }
  await patchConfig({ lastDate: dates[dates.length[-1]] })
}

exports.stop = async () => {
  _stopped = true
}
