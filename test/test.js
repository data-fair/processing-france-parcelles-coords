process.env.NODE_ENV = 'test'
const config = require('config')
const axios = require('axios')
const chalk = require('chalk')
const moment = require('moment')
const fs = require('fs-extra')
const assert = require('assert').strict
const processing = require('../')

const axiosInstance = axios.create()
axiosInstance.interceptors.request.use(cfg => {
  if (!/^https?:\/\//i.test(cfg.url)) {
    if (cfg.url.startsWith('/')) cfg.url = config.dataFairUrl + cfg.url
    else cfg.url = config.dataFairUrl + '/' + cfg.url
  }
  if (cfg.url.startsWith(config.dataFairUrl)) {
    cfg.headers['x-apiKey'] = config.dataFairAPIKey
  }
  return cfg
}, error => Promise.reject(error))
// customize axios errors for shorter stack traces when a request fails
axiosInstance.interceptors.response.use(response => response, error => {
  if (!error.response) return Promise.reject(error)
  delete error.response.request
  delete error.response.headers
  error.response.config = { method: error.response.config.method, url: error.response.config.url, data: error.response.config.data }
  if (error.response.config.data && error.response.config.data._writableState) delete error.response.config.data
  if (error.response.data && error.response.data._readableState) delete error.response.data
  return Promise.reject(error.response)
})

describe('France parcelles coords processing', () => {
  it('should expose a plugin config schema for super admins', async () => {
    const schema = require('../plugin-config-schema.json')
    assert.equal(Object.keys(schema.properties).length, 0)
  })

  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.equal(schema.type, 'object')
  })

  it('should run a task', async function () {
    this.timeout(120000)

    const pluginConfig = {}
    const processingConfig = {
      datasetMode: 'create',
      dataset: { title: 'france parcelles test', id: 'cadastre-parcelles-coords' },
      deps: ['976']
    }
    const log = {
      step: (msg) => console.log(chalk.blue.bold.underline(`[${moment().format('LTS')}] ${msg}`)),
      error: (msg, extra) => console.log(chalk.red.bold(`[${moment().format('LTS')}] ${msg}`), extra),
      warning: (msg, extra) => console.log(chalk.red(`[${moment().format('LTS')}] ${msg}`), extra),
      info: (msg, extra) => console.log(chalk.blue(`[${moment().format('LTS')}] ${msg}`), extra),
      debug: (msg, extra) => {
        // console.log(`[${moment().format('LTS')}] ${msg}`, extra)
      }
    }
    const patchConfig = async (patch) => {
      console.log('received config patch', patch)
      Object.assign(processingConfig, patch)
    }
    await fs.ensureDir('data/tmp')
    await processing.run({ pluginConfig, processingConfig, axios: axiosInstance, log, patchConfig, tmpDir: 'data/tmp' })
    assert.equal(processingConfig.datasetMode, 'update')
    const datasetId = processingConfig.dataset.id
    assert.ok(datasetId.startsWith('france-parcelles-test'))
  })
})
