/** A completion attestation from a trusted, instrumented SDK resource owner. */
export type ResourceCleanupResult =
	| { status: 'clean' }
	| { status: 'uncertain'; reason: 'cleanup_failed' | 'untracked_resource' | 'generation_mismatch' }

export interface CleanupResource<T> {
	value: T
	/** Request shutdown; admission is already closed when this callback runs. */
	stop(this: void, options: { deadlineMs: number }): void | Promise<void>
	/** Await actual producer/resource completion, not merely cancellation or leader exit. */
	waitForCleanup(this: void, options: { deadlineMs: number }): Promise<ResourceCleanupResult>
}

export type CleanupReceipt = {
	version: 1
	runId: string
	generation: string
	scope: 'sdk-owned-writers'
} & (
	| { status: 'clean'; resources: readonly string[] }
	| {
			status: 'uncertain'
			reason: 'deadline' | 'cleanup_failed' | 'untracked_resource' | 'generation_mismatch'
			pendingResources: readonly string[]
	  }
)

export interface StopAndAwaitCleanupOptions {
	deadlineMs: number
	/** A stale caller cannot stop a newer generation. */
	expectedGeneration?: string
}

type ResourceRecord = {
	ready: Promise<Omit<CleanupResource<unknown>, 'value'> | undefined>
	done: boolean
}

/**
 * In-memory ownership ledger for one run. Registration is reserved before activation.
 * This is an instrumentation contract, not OS containment or a discovery mechanism.
 * Every uninstrumented creation path must mark uncertainty before it can create work.
 */
export class RunCleanup {
	private readonly identity: Readonly<{ runId: string; generation: string }>
	private readonly records = new Map<string, ResourceRecord>()
	private readonly unknown = new Map<string, 'cleanup_failed' | 'untracked_resource' | 'generation_mismatch'>()
	private open = true
	private producerStarted = false
	private producerSettled = false
	private producerCompletion: Promise<void> | undefined
	private stopPromise: Promise<CleanupReceipt> | undefined

	constructor(
		private readonly requestStop: () => void,
		readonly parent?: RunCleanup,
	) {
		this.identity = Object.freeze({
			runId: crypto.randomUUID(),
			generation: parent?.generation ?? crypto.randomUUID(),
		})
	}

	get runId(): string {
		return this.identity.runId
	}

	get generation(): string {
		return this.identity.generation
	}

	get acceptingResources(): boolean {
		return this.open
	}

	markUncertain(
		id: string,
		reason: 'cleanup_failed' | 'untracked_resource' | 'generation_mismatch' = 'untracked_resource',
	): void {
		this.unknown.set(id, reason)
	}

	/** @internal The SDK must attach its producer before it can execute. */
	startProducer(start: () => Promise<void>): void {
		if (!this.open || this.producerStarted) throw new Error('Cleanup producer admission is closed')
		this.producerStarted = true
		let complete!: () => void
		this.producerCompletion = new Promise<void>((resolve) => {
			complete = resolve
		})
		try {
			Promise.resolve(start()).then(
				() => {
					this.producerSettled = true
					complete()
				},
				() => {
					this.markUncertain(`producer:${this.runId}`, 'cleanup_failed')
					this.producerSettled = true
					complete()
				},
			)
		} catch {
			this.markUncertain(`producer:${this.runId}`, 'cleanup_failed')
			this.producerSettled = true
			complete()
		}
	}

	startResource<T>(id: string, start: () => CleanupResource<T>): T {
		if (!this.open) throw new Error('Cleanup resource admission is closed')
		if (!id || this.records.has(id)) {
			this.markUncertain(id || 'invalid-resource', 'untracked_resource')
			throw new Error('Cleanup resource identity must be nonempty and unique')
		}
		let ready!: (resource: Omit<CleanupResource<unknown>, 'value'> | undefined) => void
		const record: ResourceRecord = {
			ready: new Promise((resolve) => {
				ready = resolve
			}),
			done: false,
		}
		this.records.set(id, record)
		try {
			const resource = start()
			// Retain shutdown callbacks, not potentially large tool results or child AgentRuns.
			ready({ stop: resource.stop, waitForCleanup: resource.waitForCleanup })
			return resource.value
		} catch (error) {
			this.markUncertain(id, 'cleanup_failed')
			ready(undefined)
			throw error
		}
	}

	stopAndAwaitCleanup(options: StopAndAwaitCleanupOptions): Promise<CleanupReceipt> {
		if (options.expectedGeneration !== undefined && options.expectedGeneration !== this.generation) {
			return Promise.resolve(this.receipt('generation_mismatch'))
		}
		if (!Number.isSafeInteger(options.deadlineMs) || options.deadlineMs < 0 || options.deadlineMs > 2_147_483_647) {
			throw new RangeError('deadlineMs must be a nonnegative bounded integer')
		}
		if (this.stopPromise) return this.stopPromise
		// Reserve the shared operation before abort callbacks can reenter.
		let resolveStop!: (receipt: CleanupReceipt) => void
		this.stopPromise = new Promise((resolve) => {
			resolveStop = resolve
		})
		// Close synchronously, before abort listeners or any asynchronous shutdown callback.
		this.open = false
		const deadline = performance.now() + options.deadlineMs
		const remaining = () => Math.max(0, Math.floor(deadline - performance.now()))
		try {
			this.requestStop()
		} catch {
			this.markUncertain('stop-callback', 'cleanup_failed')
		}
		if (!this.producerStarted) this.markUncertain('producer:not-attached')
		const waits = [...this.records].map(async ([id, record]) => {
			const resource = await record.ready
			if (!resource) return
			try {
				await resource.stop({ deadlineMs: remaining() })
				const result = await resource.waitForCleanup({ deadlineMs: remaining() })
				if (result.status === 'clean') record.done = true
				else this.markUncertain(id, result.reason)
			} catch {
				this.markUncertain(id, 'cleanup_failed')
			}
		})
		let timer: ReturnType<typeof setTimeout> | undefined
		const timedOut = new Promise<CleanupReceipt>((resolve) => {
			timer = setTimeout(() => resolve(this.receipt('deadline')), remaining())
		})
		void Promise.race([
			Promise.all([this.producerCompletion, ...waits]).then(() =>
				this.receipt(performance.now() > deadline ? 'deadline' : undefined),
			),
			timedOut,
		]).then((receipt) => {
			if (timer) clearTimeout(timer)
			resolveStop(receipt)
		})
		return this.stopPromise
	}

	private receipt(forcedReason?: 'deadline' | 'generation_mismatch'): CleanupReceipt {
		const base = {
			version: 1 as const,
			runId: this.runId,
			generation: this.generation,
			scope: 'sdk-owned-writers' as const,
		}
		if (
			!forcedReason &&
			this.producerStarted &&
			this.producerSettled &&
			!this.unknown.size &&
			[...this.records.values()].every((record) => record.done)
		) {
			return Object.freeze({
				...base,
				status: 'clean',
				resources: Object.freeze([`producer:${this.runId}`, ...this.records.keys()]),
			})
		}
		const reason = forcedReason ?? (this.unknown.values().next().value || 'untracked_resource')
		const pendingResources = [
			...new Set([
				...(!this.producerSettled ? [`producer:${this.runId}`] : []),
				...[...this.records].filter(([, record]) => !record.done).map(([id]) => id),
				...this.unknown.keys(),
			]),
		].sort()
		return Object.freeze({
			...base,
			status: 'uncertain',
			reason,
			pendingResources: Object.freeze(pendingResources),
		})
	}
}
