import fs from 'fs-extra'
import { pipeline } from 'node:stream/promises'
import path from 'node:path'
import { Writable } from 'node:stream'
import JSONStream from 'JSONStream'
import zlib from 'node:zlib'
import pointOnFeature from '@turf/point-on-feature'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '../types/processingConfig/index.ts'

const bulkSize = 10000

const baseDataset = {
  isRest: true,
  description: `Ce jeu de données contient les codes des parcelles du plan cadastral français associés à une coordonnée géographique simple (un point sur la parcelle). Il est conçu comme une donnée de référence permettant la géolocalisation des données qui contiennent un code parcelle.
  
Tous les codes parcelles connus sont présents, y compris ceux qui ont disparus des publications les plus récentes. De cette manière il est possible de géolocaliser des données qui référencent des parcelles expirées.`,
  origin: 'https://cadastre.data.gouv.fr/datasets/cadastre-etalab',
  license: {
    title: 'Licence Ouverte / Open Licence',
    href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
  },
  schema: [
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
  ],
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
  }
}

/**
 * Download the parcelles archive of a department for a given publication date.
 * Returns undefined if this department was not published at this date.
 * maxRedirects is necessary, the axios instance of the context disables redirects by default.
 */
const fetchFile = async (axios: ProcessingContext['axios'], log: ProcessingContext['log'], date: string, dep: string, tmpDir: string, signal: AbortSignal) => {
  const tmpFile = path.join(tmpDir, `${date}-${dep}.json.gz`)
  const downloadingFile = `${tmpFile}.part`

  // this is used only in dev
  if (await fs.pathExists(tmpFile)) {
    await log.info(`Le fichier ${tmpFile} existe déjà`)
    return tmpFile
  }

  await fs.ensureFile(downloadingFile)
  const url = `https://files.data.gouv.fr/cadastre/etalab-cadastre/${date}/geojson/departements/${dep}/cadastre-${dep}-parcelles.json.gz`
  await log.info('Télécharge le fichier ' + url)
  try {
    const res = await axios.get(url, { responseType: 'stream', maxRedirects: 5, signal })
    await pipeline(res.data, fs.createWriteStream(downloadingFile), { signal })
    await fs.move(downloadingFile, tmpFile, { overwrite: true })
  } catch (err: any) {
    await fs.remove(downloadingFile)
    if (err.status === 404 || err.response?.status === 404) {
      return
    }
    throw err
  }

  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(tmpFile, 'r')
  await fs.fsync(fd)
  await fs.close(fd)

  return tmpFile
}

/**
 * Read the parcelles of an archive and keep 1 point per parcelle code.
 * Publications are read in chronological order, so a code seen again in a more
 * recent publication simply overwrites its previous position.
 */
const readFile = async (tmpFile: string, coords: Map<string, string>, signal: AbortSignal) => {
  let nbParcelles = 0
  await pipeline(
    fs.createReadStream(tmpFile),
    zlib.createUnzip(),
    JSONStream.parse('features.*'),
    new Writable({
      objectMode: true,
      write (parcelle: any, _encoding, callback) {
        const point = pointOnFeature(parcelle)
        coords.set(parcelle.properties.id, `${point.geometry.coordinates[1]},${point.geometry.coordinates[0]}`)
        nbParcelles++
        callback()
      }
    }),
    { signal }
  )
  return nbParcelles
}

let _stopped = false
let _abortController: AbortController | undefined

export const run = async (context: ProcessingContext<ProcessingConfig>) => {
  const { processingConfig, processingId, dir, tmpDir, axios, log, patchConfig } = context
  _stopped = false
  _abortController = new AbortController()
  const signal = _abortController.signal

  let dataset: any
  if (processingConfig.datasetMode === 'create') {
    await log.step('Création du jeu de données')
    const body = {
      ...baseDataset,
      title: processingConfig.dataset?.title,
      extras: { processingId }
    }
    if (processingConfig.dataset?.id) {
      try {
        await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)
        throw new Error('le jeu de données existe déjà')
      } catch (err: any) {
        if (err.status !== 404 && err.response?.status !== 404) throw err
      }
      dataset = (await axios.put('api/v1/datasets/' + processingConfig.dataset.id, body)).data
    } else {
      dataset = (await axios.post('api/v1/datasets', body)).data
    }
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  } else if (processingConfig.datasetMode === 'update') {
    await log.step('Vérification du jeu de données')
    if (!processingConfig.dataset?.id) throw new Error('Identifiant de jeu de données manquant')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  await log.step('Vérification des dates de publication')
  let datesHtml: string
  try {
    datesHtml = (await axios.get('https://files.data.gouv.fr/cadastre/etalab-cadastre/', { maxRedirects: 5, signal })).data
  } catch (err: any) {
    await log.warning('échec de la lecture du domaine principal, repli sur cadastre.data.gouv.fr', err.message)
    datesHtml = (await axios.get('https://cadastre.data.gouv.fr/data/etalab-cadastre/', { maxRedirects: 5, signal })).data
  }
  const dates = [...new Set([...datesHtml.matchAll(/[0-9]{4}-[0-9]{2}-[0-9]{2}/g)].map(m => m[0]))].sort()
  await log.info('dates de publications : ' + dates.join(', '))

  const lastProcessedDatesPath = path.join(dir, 'last-processed-dates.json')
  let lastProcessedDates: Record<string, string> = {}
  if (await fs.pathExists(lastProcessedDatesPath)) {
    lastProcessedDates = await fs.readJson(lastProcessedDatesPath)
    for (const key in lastProcessedDates) {
      await log.info(`dernière date de traitement pour le département ${key} = ${lastProcessedDates[key]}`)
    }
  }

  const deps = processingConfig.deps ?? []
  try {
    for (const dep of deps) {
      await log.step(`Traitement du département ${dep}`)
      const lastProcessedDate = lastProcessedDates[dep]
      if (lastProcessedDate) await log.info(`ce département a déjà été traité jusqu'à la date ${lastProcessedDate}`)
      const depDates = lastProcessedDate ? dates.filter(d => d > lastProcessedDate) : dates
      if (!depDates.length) {
        await log.info('aucune nouvelle publication')
        continue
      }

      const coords = new Map<string, string>()
      // the next archive is downloaded while the current one is being read
      let pendingFetch: Promise<string | undefined> | undefined
      const startFetch = (date: string) => {
        const promise = fetchFile(axios, log, date, dep, tmpDir, signal)
        // the result is always awaited below, this only prevents an unhandled
        // rejection if we bail out (stop or error) before getting there
        promise.catch(() => {})
        return promise
      }

      const readTask = `Lecture des publications du département ${dep}`
      await log.task(readTask)
      try {
        pendingFetch = startFetch(depDates[0])
        for (let i = 0; i < depDates.length; i++) {
          const tmpFile = await pendingFetch
          pendingFetch = i + 1 < depDates.length ? startFetch(depDates[i + 1]) : undefined
          if (tmpFile) {
            const nbParcelles = await readFile(tmpFile, coords, signal)
            await log.info(`${nbParcelles} parcelles lues dans la publication ${depDates[i]}`)
            await fs.remove(tmpFile)
          } else {
            await log.info(`pas de fichier pour la publication ${depDates[i]}`)
          }
          await log.progress(readTask, i + 1, depDates.length)
        }
      } finally {
        // never leave a download running after the end of the department
        if (pendingFetch) await pendingFetch.catch(() => {})
      }

      if (!coords.size) {
        await log.info('aucune ligne à importer')
        continue
      }

      const sendTask = `Envoi des lignes du département ${dep}`
      await log.task(sendTask)
      let nbSent = 0
      let lines: { _id: string, code: string, coord: string }[] = []
      const sendLines = async () => {
        const res = await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
        if (res.data.nbErrors) {
          await log.error(`${res.data.nbErrors} échecs sur ${lines.length} lignes à insérer`, res.data.errors)
          throw new Error('échec à l\'insertion des lignes dans le jeu de données')
        }
        nbSent += lines.length
        lines = []
        await log.progress(sendTask, nbSent, coords.size)
      }
      for (const [code, coord] of coords) {
        if (_stopped) {
          await log.info('interruption demandée')
          return
        }
        lines.push({ _id: code, code, coord })
        if (lines.length === bulkSize) await sendLines()
      }
      if (lines.length) await sendLines()
      coords.clear()

      lastProcessedDates[dep] = dates[dates.length - 1]
      await log.info(`mémorise la dernière date traitée ${dates[dates.length - 1]}`)
      await fs.writeJson(lastProcessedDatesPath, lastProcessedDates, { spaces: 2 })
    }
  } catch (err) {
    // downloads and reads are interrupted by aborting the signal, the resulting
    // error is expected and should not fail the processing
    if (_stopped) {
      await log.info('interruption demandée')
      return
    }
    throw err
  }
}

export const stop = async () => {
  _stopped = true
  _abortController?.abort()
}
