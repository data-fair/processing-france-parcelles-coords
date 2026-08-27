import config from '#config'
import { strict as assert } from 'node:assert'
import { it, describe } from 'node:test'
import path from 'node:path'
import fs from 'fs-extra'
import axios from 'axios'
import testUtils from '@data-fair/lib-processing-dev/tests-utils.js'
import * as plugin from '../index.ts'
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

describe('France parcelles coords processing', () => {
  it('should expose a processing config schema for users', () => {
    assert.equal(processingConfigSchema.type, 'object')
  })

  it('should run a task and download parcelles with redirect support', async function () {
    await fs.emptyDir('data/tmp')
    await fs.ensureDir('data/dir')

    // Seed lastProcessedDates so integration test only fetches the latest publication date
    await fs.writeJson(path.join('data/dir', 'last-processed-dates.json'), {
      976: '2026-03-01'
    })

    const context = testUtils.context({
      tmpDir: 'data/tmp',
      dir: 'data/dir',
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'france parcelles test' },
        deps: ['976']
      }
    }, config, false)

    const sentLines: any[] = []
    const httpAdapter = axios.getAdapter('http')

    context.axios.defaults.adapter = async (reqConfig) => {
      const url = reqConfig.url || ''
      if (url.includes('/api/v1/datasets') && reqConfig.method?.toLowerCase() === 'post') {
        if (url.endsWith('/_bulk_lines')) {
          const body = typeof reqConfig.data === 'string' ? JSON.parse(reqConfig.data) : reqConfig.data
          sentLines.push(...body)
          return { status: 200, statusText: 'OK', headers: {}, config: reqConfig, data: { nbOk: body.length, nbErrors: 0 } }
        }
        return { status: 200, statusText: 'OK', headers: {}, config: reqConfig, data: { id: 'france-parcelles-test', title: 'france parcelles test' } }
      }
      return httpAdapter(reqConfig)
    }

    await plugin.run(context)
    assert.equal(context.processingConfig.datasetMode, 'update')
    assert.equal(context.processingConfig.dataset.id, 'france-parcelles-test')
    assert.ok(sentLines.length > 0, 'Should have processed and sent parcel coordinates')
    assert.ok(sentLines[0].code, 'Line should have code')
    assert.ok(sentLines[0].coord, 'Line should have coord')
    assert.match(sentLines[0].coord, /^-?\d+\.\d+,-?\d+\.\d+$/)
  })

  it('should stop quickly when the run is interrupted', async function () {
    await fs.emptyDir('data/tmp')
    await fs.ensureDir('data/dir')

    // several publications are pending, so the run would last minutes without a graceful stop
    await fs.writeJson(path.join('data/dir', 'last-processed-dates.json'), {
      976: '2025-01-01'
    })

    const context = testUtils.context({
      tmpDir: 'data/tmp',
      dir: 'data/dir',
      processingConfig: {
        datasetMode: 'create',
        dataset: { title: 'france parcelles test' },
        deps: ['976']
      }
    }, config, false)

    const sentLines: any[] = []
    const httpAdapter = axios.getAdapter('http')
    context.axios.defaults.adapter = async (reqConfig) => {
      const url = reqConfig.url || ''
      if (url.includes('/api/v1/datasets') && reqConfig.method?.toLowerCase() === 'post') {
        if (url.endsWith('/_bulk_lines')) {
          const body = typeof reqConfig.data === 'string' ? JSON.parse(reqConfig.data) : reqConfig.data
          sentLines.push(...body)
          return { status: 200, statusText: 'OK', headers: {}, config: reqConfig, data: { nbOk: body.length, nbErrors: 0 } }
        }
        return { status: 200, statusText: 'OK', headers: {}, config: reqConfig, data: { id: 'france-parcelles-test', title: 'france parcelles test' } }
      }
      return httpAdapter(reqConfig)
    }

    // ask for an interruption as soon as the first archive starts being downloaded
    const info = context.log.info
    context.log.info = async (msg: string, extra?: any) => {
      await info(msg, extra)
      if (msg.startsWith('Télécharge le fichier')) await plugin.stop()
    }

    const start = Date.now()
    await plugin.run(context)
    const duration = Date.now() - start

    assert.ok(duration < 30000, `the run should be interrupted quickly, took ${duration}ms`)
    assert.equal(sentLines.length, 0, 'no line should be sent after an interruption')
    const lastProcessedDates = await fs.readJson(path.join('data/dir', 'last-processed-dates.json'))
    assert.equal(lastProcessedDates['976'], '2025-01-01', 'an interrupted department should not be marked as processed')
  })
})
