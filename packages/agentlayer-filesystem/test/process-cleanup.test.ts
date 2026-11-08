import { describe, expect, test } from 'bun:test'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { Agent, startState } from '@humanlayer/agentlayer-core'
import { mockModel } from '../../agentlayer-core/test/mocks'
import { runProcess } from '../src/utils/process'

async function runWithoutModel() {
	const run = new Agent({ model: mockModel([]), tools: {}, maxSteps: 0 }).run({ state: startState([]) })
	await run.result
	return run
}
async function eventually(check: () => Promise<boolean>) {
	const deadline = performance.now() + 3000
	while (performance.now() < deadline) {
		if (await check()) return
		await delay(5)
	}
	throw new Error('Fixture did not become ready')
}

describe('process cleanup ownership', () => {
	test('default command behavior remains intact, while tracked shell scope stays uncertain', async () => {
		const ordinary = await runProcess(process.execPath, ['-e', 'console.log("ordinary")'])
		expect(ordinary.exitCode).toBe(0)
		expect(ordinary.stdout.trim()).toBe('ordinary')
		const run = await runWithoutModel()
		const tracked = await runProcess(process.execPath, ['-e', 'console.log("tracked")'], { cleanup: run.cleanup })
		expect(tracked.exitCode).toBe(0)
		expect(tracked.stdout.trim()).toBe('tracked')
		const receipt = await run.stopAndAwaitCleanup({ deadlineMs: 1000 })
		expect(receipt).toMatchObject({ status: 'uncertain', reason: 'untracked_resource' })
	})

	test('escaped detached writer survives leader exit, so receipt cannot claim clean', async () => {
		const dir = await mkdtemp(join(tmpdir(), 'sdk-detached-'))
		let pid: number | undefined
		try {
			const file = join(dir, 'writes')
			const pidFile = join(dir, 'owned-pid')
			const writer = `const fs=require('node:fs');setInterval(()=>fs.appendFileSync(${JSON.stringify(file)},'w'),5);process.on('SIGTERM',()=>process.exit(0))`
			const leader = `const {spawn}=require('node:child_process');const fs=require('node:fs');const child=spawn(process.execPath,['-e',${JSON.stringify(writer)}],{detached:true,stdio:'ignore'});fs.writeFileSync(${JSON.stringify(pidFile)},String(child.pid));child.unref()`
			const run = await runWithoutModel()
			const result = await runProcess(process.execPath, ['-e', leader], { cleanup: run.cleanup })
			expect(result.exitCode).toBe(0)
			pid = Number(await readFile(pidFile, 'utf8'))
			expect(Number.isSafeInteger(pid) && pid > 1).toBe(true)
			await eventually(async () => (await readFile(file, 'utf8').catch(() => '')).length > 0)
			const receipt = await run.stopAndAwaitCleanup({ deadlineMs: 1000 })
			expect(receipt.status).toBe('uncertain')
			const before = (await readFile(file, 'utf8')).length
			await eventually(async () => (await readFile(file, 'utf8')).length > before)
		} finally {
			// The PID comes only from this exact private fixture's spawn; no process-group sweep.
			if (pid) {
				try {
					process.kill(pid, 'SIGTERM')
				} catch {}
			}
			await rm(dir, { recursive: true, force: true })
		}
	}, 10_000)

	test('closed admission rejects a subprocess before activation', async () => {
		const run = await runWithoutModel()
		expect((await run.stopAndAwaitCleanup({ deadlineMs: 100 })).status).toBe('clean')
		await expect(
			runProcess(process.execPath, ['-e', 'throw new Error("must not start")'], { cleanup: run.cleanup }),
		).rejects.toThrow('admission is closed')
	})
})
