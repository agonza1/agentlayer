import { describe, expect, test } from 'bun:test'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { MockLanguageModelV3 } from 'ai/test'
import { z } from 'zod'
import { Agent, AgentRun, defineTool, RunCleanup, startState } from '../src'
import { assistantText, assistantWithToolCall, mockModel } from './mocks'

const TIMEOUT = 10_000
const idleAgent = () => new Agent({ model: mockModel([]), tools: {}, maxSteps: 0 })
async function settledRun() {
	const run = idleAgent().run({ state: startState([]) })
	await run.result
	return run
}
async function fixture<T>(fn: (dir: string) => Promise<T>) {
	const dir = await mkdtemp(join(tmpdir(), 'sdk-cleanup-'))
	try {
		return await fn(dir)
	} finally {
		await rm(dir, { recursive: true, force: true })
	}
}
async function eventually(check: () => Promise<boolean>) {
	const end = performance.now() + 3000
	while (performance.now() < end) {
		if (await check()) return
		await delay(5)
	}
	throw new Error('Fixture did not become ready')
}

/** A trusted finite supervisor adapter; it owns and awaits the exact grandchild it starts. */
function startFiniteTree(scope: RunCleanup, file: string) {
	let child: ReturnType<typeof spawn> | undefined
	const result = scope.startResource('finite-supervisor', () => {
		const writer = `const fs=require('node:fs');setInterval(()=>fs.appendFileSync(${JSON.stringify(file)},'w'),5);process.on('SIGTERM',()=>process.exit(0))`
		const script = `const {spawn}=require('node:child_process');const child=spawn(process.execPath,['-e',${JSON.stringify(writer)}],{stdio:'ignore'});process.stdin.once('data',()=>{child.once('close',()=>{process.stdout.write('grandchild-closed\\n',()=>process.exit(0))});child.kill('SIGTERM')})`
		child = spawn(process.execPath, ['-e', script], { stdio: ['pipe', 'pipe', 'inherit'] })
		let output = ''
		child.stdout!.on('data', (data) => {
			output += data.toString()
		})
		const closed = once(child, 'close')
		return {
			value: child,
			stop: () => {
				child!.stdin!.end('stop\n')
			},
			waitForCleanup: async () => {
				const [code] = await closed
				return code === 0 && output.includes('grandchild-closed')
					? { status: 'clean' as const }
					: { status: 'uncertain' as const, reason: 'cleanup_failed' as const }
			},
		}
	})
	return { child: result, emergencyStop: () => child?.stdin?.end('stop\n') }
}

describe('generation-owned cleanup prototype', () => {
	test('an empty ledger without an attached producer is never proof', async () => {
		const scope = new RunCleanup(() => {})
		const receipt = await scope.stopAndAwaitCleanup({ deadlineMs: 100 })
		expect(receipt).toMatchObject({ status: 'uncertain', reason: 'untracked_resource' })
	})

	test(
		'awaits actual nested run and finite subprocess/grandchild completion',
		async () => {
			await fixture(async (dir) => {
				const parent = idleAgent().run({ state: startState([]) })
				const child = idleAgent().run({ state: startState([]), cleanupParent: parent.cleanup })
				const grandchild = idleAgent().run({ state: startState([]), cleanupParent: child.cleanup })
				expect(child.generation).toBe(parent.generation)
				expect(grandchild.generation).toBe(parent.generation)
				expect(new Set([parent.runId, child.runId, grandchild.runId]).size).toBe(3)
				const file = join(dir, 'writes')
				const tree = startFiniteTree(grandchild.cleanup, file)
				try {
					await eventually(async () => (await readFile(file, 'utf8').catch(() => '')).length > 1)
					const receipt = await parent.stopAndAwaitCleanup({ deadlineMs: 3000 })
					expect(receipt.status).toBe('clean')
					expect(tree.child.exitCode).toBe(0)
					const stopped = await readFile(file, 'utf8')
					await delay(30)
					expect(await readFile(file, 'utf8')).toBe(stopped)
					expect(Object.isFrozen(receipt)).toBe(true)
				} finally {
					tree.emergencyStop()
				}
			})
		},
		TIMEOUT,
	)

	test('seals admission before stop callbacks and shares reentrant/concurrent stops', async () => {
		const run = await settledRun()
		let reentered: Promise<unknown> | undefined
		let activated = false
		run.cleanup.startResource('resource', () => ({
			value: undefined,
			stop: () => {
				expect(run.cleanup.acceptingResources).toBe(false)
				expect(() =>
					run.cleanup.startResource('late', () => {
						activated = true
						throw new Error('unreachable')
					}),
				).toThrow('admission is closed')
				reentered = run.stopAndAwaitCleanup({ deadlineMs: 1000 })
			},
			waitForCleanup: async () => ({ status: 'clean' }),
		}))
		const first = run.stopAndAwaitCleanup({ deadlineMs: 1000 })
		const second = run.stopAndAwaitCleanup({ deadlineMs: 1 })
		expect(first).toBe(second)
		expect((await first).status).toBe('clean')
		expect(reentered).toBe(first)
		expect(activated).toBe(false)
	})

	test('a stop during synchronous activation waits for the reserved resource', async () => {
		const run = await settledRun()
		let stop: Promise<unknown> | undefined
		let stopped = false
		run.cleanup.startResource('activation-race', () => {
			stop = run.stopAndAwaitCleanup({ deadlineMs: 1000 })
			return {
				value: undefined,
				stop: () => {
					stopped = true
				},
				waitForCleanup: async () => ({ status: 'clean' }),
			}
		})
		expect(await stop).toMatchObject({ status: 'clean' })
		expect(stopped).toBe(true)
	})

	test('stale generations cannot abort the current run', async () => {
		const run = new AgentRun()
		let aborted = false
		let finish!: () => void
		run.startProducer(
			(signal) =>
				new Promise<void>((resolve) => {
					finish = resolve
					signal.addEventListener('abort', () => {
						aborted = true
						resolve()
					})
				}),
		)
		const wrong = await run.stopAndAwaitCleanup({ deadlineMs: 100, expectedGeneration: crypto.randomUUID() })
		expect(wrong).toMatchObject({ status: 'uncertain', reason: 'generation_mismatch', generation: run.generation })
		expect(aborted).toBe(false)
		expect(run.cleanup.acceptingResources).toBe(true)
		finish()
		expect((await run.stopAndAwaitCleanup({ deadlineMs: 100, expectedGeneration: run.generation })).status).toBe(
			'clean',
		)
	})

	test('producer/result settlement does not hide an incomplete resource or renew a deadline', async () => {
		const run = await settledRun()
		let finish!: () => void
		const completed = new Promise<void>((resolve) => {
			finish = resolve
		})
		run.cleanup.startResource('still-writing', () => ({
			value: undefined,
			stop: () => {},
			waitForCleanup: async () => {
				await completed
				return { status: 'clean' }
			},
		}))
		const first = run.stopAndAwaitCleanup({ deadlineMs: 10 })
		const receipt = await first
		expect(receipt).toMatchObject({ status: 'uncertain', reason: 'deadline', pendingResources: ['still-writing'] })
		finish()
		expect(run.stopAndAwaitCleanup({ deadlineMs: 1000 })).toBe(first)
		expect(await first).toBe(receipt)
	})

	test('an expired zero deadline cannot turn later microtask settlement into clean', async () => {
		const run = await settledRun()
		expect(await run.stopAndAwaitCleanup({ deadlineMs: 0 })).toMatchObject({
			status: 'uncertain',
			reason: 'deadline',
		})
	})

	test.each(['spawn', 'cleanup'] as const)('%s failures cannot produce clean', async (failure) => {
		const run = await settledRun()
		if (failure === 'spawn') {
			expect(() =>
				run.cleanup.startResource('failed', () => {
					throw new Error('injected spawn failure')
				}),
			).toThrow()
		} else {
			run.cleanup.startResource('failed', () => ({
				value: undefined,
				stop: () => {
					throw new Error('injected cleanup failure')
				},
				waitForCleanup: async () => ({ status: 'clean' }),
			}))
		}
		expect(await run.stopAndAwaitCleanup({ deadlineMs: 100 })).toMatchObject({
			status: 'uncertain',
			reason: 'cleanup_failed',
		})
	})

	test('interrupted caller does not cancel the shared shutdown', async () => {
		const run = await settledRun()
		let stopped = false
		run.cleanup.startResource('owned-callback', () => ({
			value: undefined,
			stop: async () => {
				await delay(30)
				stopped = true
			},
			waitForCleanup: async () => ({ status: 'clean' }),
		}))
		const shutdown = run.stopAndAwaitCleanup({ deadlineMs: 1000 })
		await expect(
			Promise.race([
				shutdown,
				delay(5).then(() => {
					throw new Error('caller interrupted')
				}),
			]),
		).rejects.toThrow('caller interrupted')
		expect(run.stopAndAwaitCleanup({ deadlineMs: 1000 })).toBe(shutdown)
		expect((await shutdown).status).toBe('clean')
		expect(stopped).toBe(true)
	})

	test('generic tool and model promise completion retain unsupported ownership', async () => {
		await fixture(async (dir) => {
			const file = join(dir, 'late')
			let completed!: () => void
			const late = new Promise<void>((resolve) => {
				completed = resolve
			})
			const tool = defineTool({
				name: 'late',
				description: 'Fixture with uninstrumented callback',
				input: z.object({}),
				execute: async () => {
					setTimeout(() => {
						void writeFile(file, 'late').then(completed)
					}, 40)
					return 'returned'
				},
			})
			const agent = new Agent({
				model: mockModel([assistantWithToolCall('late', {}), assistantText('done')]),
				tools: { late: tool },
			})
			const run = agent.run({ state: startState([{ role: 'user', content: 'fixture' }]) })
			await run.result
			const receipt = await run.stopAndAwaitCleanup({ deadlineMs: 1000 })
			expect(receipt.status).toBe('uncertain')
			if (receipt.status === 'uncertain') {
				expect(receipt.pendingResources).toContain('model-provider-transport')
				expect(receipt.pendingResources.some((id) => id.endsWith(':uninstrumented'))).toBe(true)
			}
			await late
			expect(await readFile(file, 'utf8')).toBe('late')
		})
	})

	test.each(['stop', 'provider-options'] as const)(
		'configured %s callbacks cannot hide late writers in a no-model run',
		async (kind) => {
			await fixture(async (dir) => {
				const file = join(dir, kind)
				let completed!: () => void
				const done = new Promise<void>((resolve) => {
					completed = resolve
				})
				const callback = () => {
					setTimeout(() => {
						void writeFile(file, 'late').then(completed)
					}, 30)
				}
				const agent = new Agent({
					model: mockModel([]),
					tools: {},
					maxSteps: 0,
					...(kind === 'stop'
						? { onStop: callback }
						: {
								providerOptions: () => {
									callback()
									return {}
								},
							}),
				})
				const run = agent.run({ state: startState([]) })
				await run.result
				expect(await run.stopAndAwaitCleanup({ deadlineMs: 1000 })).toMatchObject({
					status: 'uncertain',
					pendingResources: ['configured-extensions'],
				})
				await done
				expect(await readFile(file, 'utf8')).toBe('late')
			})
		},
	)

	test('closed admission suppresses a newly dispatched event observer', async () => {
		const run = await settledRun()
		let called = false
		run.onEvent = () => {
			called = true
		}
		expect((await run.stopAndAwaitCleanup({ deadlineMs: 1000 })).status).toBe('clean')
		run.pushEvent({ type: 'stepStart', stepIndex: 0 })
		expect(called).toBe(false)
	})

	test(
		'local HTTP late callback is not hidden by a closed socket/server',
		async () => {
			await fixture(async (dir) => {
				const run = await settledRun()
				let requestSeen!: () => void
				const seen = new Promise<void>((resolve) => {
					requestSeen = resolve
				})
				let callbackDone!: () => void
				const done = new Promise<void>((resolve) => {
					callbackDone = resolve
				})
				const file = join(dir, 'http-late')
				const server = run.cleanup.startResource('http', () => {
					run.cleanup.markUncertain('http:uninstrumented-callback')
					const server = createServer((_, response) => {
						response.end('ok')
						requestSeen()
						setTimeout(() => {
							void writeFile(file, 'late').then(callbackDone)
						}, 40)
					})
					server.listen(0, '127.0.0.1')
					const closed = new Promise<void>((resolve) => server.once('close', resolve))
					return {
						value: server,
						stop: () => {
							server.close()
							server.closeAllConnections()
						},
						waitForCleanup: async () => {
							await closed
							return { status: 'clean' }
						},
					}
				})
				await once(server, 'listening')
				const address = server.address()
				if (!address || typeof address === 'string') throw new Error('Missing fixture address')
				await fetch(`http://127.0.0.1:${address.port}`)
				await seen
				expect((await run.stopAndAwaitCleanup({ deadlineMs: 1000 })).status).toBe('uncertain')
				await done
				expect(await readFile(file, 'utf8')).toBe('late')
			})
		},
		TIMEOUT,
	)

	test(
		'local WebSocket close is not proof of a previously scheduled callback',
		async () => {
			await fixture(async (dir) => {
				const run = await settledRun()
				let received!: () => void
				const seen = new Promise<void>((resolve) => {
					received = resolve
				})
				let callbackDone!: () => void
				const done = new Promise<void>((resolve) => {
					callbackDone = resolve
				})
				const file = join(dir, 'ws-late')
				const server = run.cleanup.startResource('websocket', () => {
					run.cleanup.markUncertain('websocket:uninstrumented-callback')
					const server = Bun.serve({
						hostname: '127.0.0.1',
						port: 0,
						fetch: (request, server) =>
							server.upgrade(request) ? undefined : new Response('no', { status: 400 }),
						websocket: {
							message: () => {
								received()
								setTimeout(() => {
									void writeFile(file, 'late').then(callbackDone)
								}, 40)
							},
						},
					})
					return {
						value: server,
						stop: async () => {
							await server.stop(true)
						},
						waitForCleanup: async () => ({ status: 'clean' }),
					}
				})
				const socket = new WebSocket(`ws://127.0.0.1:${server.port}`)
				await new Promise<void>((resolve) => socket.addEventListener('open', () => resolve(), { once: true }))
				socket.send('fixture')
				await seen
				expect((await run.stopAndAwaitCleanup({ deadlineMs: 1000 })).status).toBe('uncertain')
				socket.close()
				await done
				expect(await readFile(file, 'utf8')).toBe('late')
			})
		},
		TIMEOUT,
	)
	test('cleanup stops an internally controlled model without aborting the caller controller', async () => {
		let started!: () => void
		const entered = new Promise<void>((resolve) => {
			started = resolve
		})
		let modelAborted = false
		const model = new MockLanguageModelV3({
			doStream: async ({ abortSignal }) => ({
				stream: new ReadableStream({
					start(controller) {
						abortSignal?.addEventListener(
							'abort',
							() => {
								modelAborted = true
								controller.error(new DOMException('Stopped', 'AbortError'))
							},
							{ once: true },
						)
						started()
					},
				}),
			}),
		})
		const caller = new AbortController()
		const run = new Agent({ model, tools: {} }).run({
			state: startState([{ role: 'user', content: 'fixture' }]),
			signal: caller.signal,
		})
		await entered
		const receipt = await run.stopAndAwaitCleanup({ deadlineMs: 1000 })
		expect(modelAborted).toBe(true)
		expect(caller.signal.aborted).toBe(false)
		expect((await run.result).finishReason).toBe('interrupted')
		expect(receipt.status).toBe('uncertain')
	})
})
