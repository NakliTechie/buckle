/**
 * Static explanations keyed by finding kind (D3: explanations are text, never
 * generated). Copy is Buckle's, in breakscale's glossary register: say what it
 * tells you and what to do. `title` heads the finding row; `why` teaches.
 */
export const EXPLANATIONS = {
  knee: {
    title: 'Utilisation knee',
    why: 'This component crosses 80% busy here. Past 80% a queue starts to build faster than it drains, so latency climbs steeply for a small rise in load. This is the first place the system stops having headroom — everything downstream of it inherits the wait.',
  },
  retry_amplification: {
    title: 'Retry amplification',
    why: 'The downstream is seeing far more traffic than the upstream is sending, because failed calls are being retried on top of the offered load. A retry storm is a feedback loop: the more the downstream struggles, the more retries pile on, which makes it struggle more. Cap retries, add jitter, or trip a breaker before this point.',
  },
  spof: {
    title: 'Single point of failure',
    why: 'Crash this one node at peak load and system goodput falls below a tenth of its peak — there is no path around it. Everything routes through this component, so its availability is the whole system\'s availability. Add a replica, a fallback, or a second path.',
  },
  p99_cliff: {
    title: 'Tail-latency cliff',
    why: 'The slowest 1% of requests get more than five times slower here than they were under light load. The average can still look fine while the tail falls off a cliff — this is where the requests that stand in line start dominating the experience.',
  },
  collapse: {
    title: 'Goodput collapse',
    why: 'Past this load the system completes less than half the useful work it did at its peak, and the failures are timeouts — it is drowning in slow work, not shedding cleanly. More load here means less done, not more.',
  },
};

export const explain = (kind) => EXPLANATIONS[kind] ?? { title: kind, why: '' };
