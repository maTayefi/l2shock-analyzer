# L2 Liquidity Shock Analyzer

Local historical order-book research application for reconstructing
CryptoHFTData L2 feeds, deriving compact liquidity observations, and
displaying verified historical data through a detector-free Analysis
workflow.

Analysis loads and renders exactly five synchronized panels: optional
Binance perpetual Price, Bid Liquidity, Ask Liquidity, selectable L2 Panel A,
and selectable L2 Panel B. It does not detect shocks, generate hypotheses,
rank areas, or draw candidate A/B/C annotations.

## Project identity

```text
Project:   L2 Liquidity Shock Analyzer
UI title:  Liquidity Shock Analyzer
Package:   l2shock
Database:  l2shock_local
Directory: C:\l2_liquidity_shock_analyzer
```

## Supported initial scope

- Base assets: BTC and ETH.
- Approved price source: Binance USD-M USDT perpetual trades.
- Price availability is optional for Analysis; missing or failed price
  loading must not prevent valid L2 data from being rendered.
- Raw source archive: CryptoHFTData hourly Parquet files.
- Database: PostgreSQL.
- UI: NiceGUI and Apache ECharts.
- Analysis: historical, offline, post-scan.
- Default UI timezone: Asia/Tehran.
- Database timestamps: UTC `timestamptz`.
- Base analytical storage resolution: one second.

## Core data flow

```text
CryptoHFTData hourly Parquet
    v
streamed row-group / record-batch reader
    v
row-level changes grouped into exchange update events
    v
per-venue and per-instrument L2 reconstruction
    v
sequence and checkpoint validation
    v
one-second reconstructed-state sampling and depth-band liquidity
    v
independent compact component-market PostgreSQL blocks
    v
verified bounded-chunk Analysis loading
    v
exact-second expected-market composition
    v
UTC-aligned viewing bars
    v
Price / Bid / Ask / selectable Panel A / selectable Panel B
```

Invalid one-second samples remain explicit. They are not populated from a
formerly valid numerical sample.

Aggregate presets resolve independently persisted component-market rows at
Analysis load time. They do not create separately persisted aggregate L2
rows or merge raw events, replay frontiers, snapshots, or checkpoints.

Binance real-trade price is independently constructed and stored. Analysis
loads it as optional context; it is not an input to L2 reconstruction or
liquidity calculation.

## Detector-free Analysis workflow

Analysis loads verified historical data and renders it. It performs no shock
detection, candidate generation, evidence scoring, ranking, or A/B/C
annotation.

The panel order is fixed:

```text
0: Binance perpetual Price, optional
1: Bid Liquidity
2: Ask Liquidity
3: selectable L2 Panel A
4: selectable L2 Panel B
```

Panel A defaults to Signed Imbalance %. Panel B defaults to Order-Book Delta.

The selectable metric registry contains exactly these eleven metrics.
In the formulas below, B and A mean Bid and Ask liquidity, not detector
anchors. T = B + A and D = B - A.

| Metric | Formula |
|---|---|
| Order-Book Delta | B - A |
| Total Liquidity | B + A |
| Signed Imbalance % | 100 * (B - A) / (B + A) |
| Bid Share % | 100 * B / (B + A) |
| Ask Share % | 100 * A / (B + A) |
| Bid/Ask Shares % (two lines) | Bid Share % and Ask Share % at bar close |
| Total Change | T[n] - T[n-1], using bar closes |
| Total Change % | 100 * (T[n] - T[n-1]) / T[n-1] |
| Delta Change | D[n] - D[n-1], using bar closes |
| Imbalance Change (pp) | Signed Imbalance %[n] - Signed Imbalance %[n-1] |
| Relative Side Change % | 100 * (B[n]/B[n-1] - A[n]/A[n-1]) |

State-metric candles are reduced from same-second values. Total and Delta
extrema are not constructed by combining independent Bid and Ask extrema.
Percentage candles are reduced from valid one-second ratios, not calculated
from separate OHLC extrema.

Undefined denominators produce null values, not zero. Change metrics require
adjacent usable viewing bars; the first bar and a bar following an unusable
bar have no change value.

No price-matched liquidity-flow mode or misleading snapshot-flow substitute
is offered. These metrics use existing preset-owned depth, not a new
Quantower level-count selection or Bookmap mid-price-relative depth rule.

Panel and warning-selector changes rebuild the currently displayed
projection without a database reread or another Start Analysis operation.
Timeframe and viewing-bar-budget changes may use cached coarsening or admit
a cancellable reload when finer data is required.

The Maximum viewing bars control is applied on blur or Enter, not on every
intermediate value change while editing.

Normal wheel interaction controls synchronized X zoom. Shift+wheel controls
independent Y zoom in the hovered panel. Changing a metric resets only that
panel's Y zoom; changing the effective viewing timeframe resets all Y zooms.
Timeframe changes preserve the captured UTC left edge and visible duration
where the new range permits, clipping to the new bounds when necessary.

Start Analysis validates the request and loads data in a background worker.
Stop Analysis is cooperative and does not clear the previously displayed
chart. A stopped or failed load does not publish a partial projection.

PNG and SVG export chart images. JSON and CSV export the full displayed-bar
dataset, not only the current browser zoom window. Exported data remains
UTC-owned; screen presentation uses the configured timezone.

### L2 Ratio Extremeness Score

Analysis also calculates one deterministic post-scan descriptive statistic:
the L2 Ratio Extremeness Score. It is not shock detection, hypothesis
generation, area ranking, evidence scoring, or an A/B/C annotation, and it is
not a causal or live signal.

Every eligible displayed viewing bar is compared with all eligible viewing
bars of the same displayed range, including later bars. Browser zoom never
changes the population; a timeframe change recomputes it from the new bars.
score[i] belongs to bar[i]; no shift is applied.

The input is the Bid Share % viewing-bar high and low. Signed Imbalance % and
Ask Share % are exact transforms of Bid Share %, so their score is identical.
Highs and lows form separate populations. Each side combines a midpoint-tie
tail fraction with a median/MAD robust distance (mean absolute deviation when
MAD is zero) mapped by z / (z + 3.5), using a geometric mean. The final
0-100 score is the larger side; BID_DOMINANT, ASK_DOMINANT, or BOTH is
separate metadata. Fewer than 20 eligible bars produce no scores.

The score is scan-relative. Scores are comparable between scan ranges only at
the same timeframe, and are not probabilities of abnormal activity.

The score is shown as a background band behind Imbalance %, Bid Share %, and
Ask Share % candle panels, and in their tooltip. One switch hides only the
background. JSON and CSV exports always include the score, side, side
components, and ratio_extremeness_algorithm_version. Nothing is persisted.

## Default remote preprocessing profile

The checked-in scheduled workflow selects Backblaze B2 for remote
processed-artifact publication:

```text
CryptoHFTData
    ->
GitHub Actions standard Linux runner
    ->
shared l2shock acquisition/replay/liquidity/price engine
    ->
private Backblaze B2 processed-artifact bucket
    ->
local verified B2 importer
    ->
local PostgreSQL
    ->
local Analysis and NiceGUI
```

Checked-in backend selection is not proof of completed production handoff.
Production readiness also requires verified seed/history ownership, exclusion
of overlapping writers, and successful live worker/import validation.

The Hugging Face adapter and credentials remain available for explicit
migration-source reads and rollback. A B2 integrity failure never silently
substitutes HF content.

The existing local workflow remains supported:

```text
CryptoHFTData
    ->
local Fetch
    ->
local Processing
    ->
local PostgreSQL
```

The remote path is intended to become the default user workflow after its
equivalence and failure-recovery tests pass. The local path remains a supported
fallback for:

```text
remote-service outage
debugging
historical reprocessing
algorithm development
artifact verification
recovery
```

The Fetch tab supports explicit Remote B2 Import and Remote HF Import.
The example local configuration retains Remote HF Import as its default
unless remote.default_workflow is explicitly changed.

B2 imports resolve one completion descriptor per artifact and retain exact
descriptor, artifact, and manifest versions. B2 has no repository-wide
commit revision or atomic range snapshot.

The retained HF importer consumes artifact/manifest pairs from one full
immutable dataset commit SHA shared by the range operation.

For each selected base and exact UTC hour, the range importer plans:

```text
Binance component L2
Bybit component L2
OKX component L2
Binance real-trade price
```

For both BTC and ETH, this is eight processed artifacts per UTC hour. Missing
artifacts remain explicit import results and are not interpreted as zero
liquidity.

The required remote artifact universe per selected UTC hour is:

```text
BTC:
    Binance Futures BTCUSDT component L2
    Bybit BTCUSDT component L2
    OKX Futures BTC-USDT-SWAP component L2
    Binance Futures BTCUSDT price

ETH:
    Binance Futures ETHUSDT component L2
    Bybit ETHUSDT component L2
    OKX Futures ETH-USDT-SWAP component L2
    Binance Futures ETHUSDT price
```

The component L2 keys use the exact existing single-market preset hashes for
the selected depth band. They do not use either aggregate preset hash.

Bybit contributes independently reconstructed component L2 only. Binance
Futures trades remain the sole remote and local price source.

The existing `Local CryptoHFTData Fetch + Processing` profile remains available
as the fallback, debugging, recovery, and reprocessing workflow.

The workflow selector is presentation state only. It does not alter preset
identity, persisted analytical content, replay semantics, or source ownership.

GitHub Actions is only an execution environment. Hugging Face is only a
persistent transport/artifact repository. They do not own a second analytical
implementation.

Hugging Face dataset commits own repository publication atomicity, while the
existing l2shock artifact key, manifest, codec, content hashes, and checkpoint
contracts own analytical correctness.

Hugging Face branch names are operational mutable references. Normal artifact
verification uses full immutable commit SHAs.

There is one authoritative implementation of:

```text
source acquisition
Parquet interpretation
order-book replay
venue sequence validation
checkpoint semantics
one-second sampling
depth liquidity
trade OHLC
compact channel encoding
provenance
content identity
```

The local application and the headless GitHub Actions CLI must call the same
modules.

The pure headless boundary accepts explicit verified local source archives,
an optional immediately preceding checkpoint, and an immutable component
preset. It returns a compact remote artifact without requiring PostgreSQL or
NiceGUI.

The headless boundary does not download files or contact Hugging Face. Those
responsibilities belong to later thin acquisition and publication adapters.

The local filesystem CLI exposes this boundary without adding networking:

```text
python -m l2shock.remote_cli l2 ...
python -m l2shock.remote_cli price ...
```

The CLI consumes source archives already stored under the canonical raw-root
layout. It writes:

```text
canonical processed transport Parquet
matching external canonical manifest
```

beneath an explicit output root.

Identical repeated publication is idempotent. Conflicting existing transport or
manifest content is rejected unless explicit overwrite was requested.

The CLI exit statuses are:

```text
0:
    processing and verified publication succeeded

1:
    unexpected implementation failure

2:
    command input, source contract, processing contract, or artifact contract
    was invalid

3:
    a required source archive or input checkpoint was unavailable

4:
    an existing artifact or manifest conflicted with the requested immutable
    result

130:
    processing was interrupted
```

The CLI still performs no CryptoHFTData download, Hugging Face access,
PostgreSQL access, or NiceGUI work.

A separate PostgreSQL-free remote-worker acquisition adapter wraps the existing
production `CryptoHFTDownloader`.

Its boundary is:

```text
explicit SourceFileSpec tuple
    ->
ephemeral worker workspace
    ->
existing CryptoHFTDownloader
    ->
structurally validated and SHA-256-owned source archives
    ->
ProcessingSourceArchive tuple
```

This adapter does not implement another downloader. It reuses the same:

```text
REST endpoint construction
anonymous or explicit API-key mode
rolling request-rate limit
retry and Retry-After handling
temporary .part files
free-disk checks
Parquet validation
SHA-256 calculation
quarantine behavior
atomic publication
```

used by local acquisition.

Scheduled remote workers must supply the newest release-eligible UTC hour to
the adapter. A requested source hour newer than that boundary is rejected
before any HTTP request.

The remote-worker workspace contains only ephemeral sibling directories:

```text
raw
cache
quarantine
exports
logs
backups
```

The worker owns the complete temporary workspace and closes it after worker
execution. Joined synchronous work finishes before cancellation can close the
workspace or B2 transport. Raw archives are not durable production storage.

The private Hugging Face dataset adapter stores one immutable processed
artifact and its matching canonical external manifest in one repository
commit.

Every read first resolves the configured dataset branch to a full immutable
commit SHA. The artifact Parquet and external manifest are then downloaded from
that same revision with:

```text
repo_type = dataset
revision = full commit SHA
```

This prevents one read from combining an artifact from one repository state
with a manifest from another state.

Every write uses optimistic concurrency:

```text
read current branch head
-> inspect immutable destination at that exact head
-> create one artifact-plus-manifest commit
-> require parent_commit to equal the observed head
```

If the branch changes concurrently, the adapter re-reads the latest head and
inspects the destination again before retrying. It never blindly reuses a stale
parent SHA.

Concurrent identical publication is idempotent success. Conflicting content at
an existing immutable artifact path is rejected.

The current Hugging Face adapter does not create repositories. The private
dataset repository must already exist, and callers must supply a suitable
token through process-secret configuration.

A fine-grained token restricted to the private dataset is preferred. Read-only
consumers require only read access; GitHub publication requires write access.
Tokens must never appear in source, workflow files, logs, manifests, artifact
content, exceptions, or object representations.

The remote-hour worker composes the existing release schedule, remote source
acquisition adapter, headless processors, and explicitly selected HF or B2
repository adapter. The checked-in workflow selects B2.
It does not add another downloader or analytical implementation.

The worker supports these chains:

```text
binance_futures / BTCUSDT:
    L2 orderbook + Binance trade price

binance_futures / ETHUSDT:
    L2 orderbook + Binance trade price

bybit / BTCUSDT:
    L2 orderbook only

bybit / ETHUSDT:
    L2 orderbook only

okx_futures / BTC-USDT-SWAP:
    L2 orderbook only

okx_futures / ETH-USDT-SWAP:
    L2 orderbook only
```

A scheduled target may not be newer than the shared release-eligible UTC-hour
boundary.

Binance Futures processing supports two empirically established initialization
paths:

1. A target archive containing a complete opening `snapshot` event may initialize
   the book independently.
2. An update-only target archive requires the immediately preceding verified
   remote L2 artifact and its output checkpoint.

For observed CryptoHFTData Binance snapshot rows, `transaction_time` and
`order_count` may be null.

The shared ingestion layer applies exactly one compatibility normalization for
Binance snapshot rows:

```text
transaction_time = event_time when transaction_time is null
```

A null `order_count` remains null. The application must not reinterpret an
unknown provider value as an explicit zero order count.

The normalization is performed in memory while streaming. The original source
archive and its source SHA-256 remain unchanged.

If the target is update-only and the required predecessor artifact or checkpoint
is absent, processing produces no usable output checkpoint and publishes no L2
artifact.

OKX may process without a predecessor because the approved OKX contract
requires the target archive to establish its own complete opening snapshot.
The target still must produce a usable output checkpoint before publication.

Bybit remote processing uses the same strict sequence contract as local Bybit
processing:

```text
update replay frontier:
    final_update_id

update continuity:
    current.final_update_id
    ==
    previous replay frontier + 1
```

A native Bybit snapshot with a non-null `final_update_id` may initialize a
remote chain.

A Bybit archive-boundary snapshot with null `final_update_id` may replace book
levels only when an immediately preceding verified checkpoint already owns the
replay frontier. Such a snapshot cannot initialize a fresh chain.

Every Bybit remote target must produce a usable output checkpoint before its L2
artifact is published.

Scheduled remote processing has three deployment gates:

```text
L2SHOCK_REMOTE_PROCESSING_ENABLED=true
    enables scheduled remote processing

L2SHOCK_BINANCE_SEEDS_READY=true
    admits Binance BTC and ETH into scheduled processing

L2SHOCK_BYBIT_SEEDS_READY=true
    admits Bybit BTC and ETH into scheduled processing
```

The Binance and Bybit seed gates independently control their two checkpoint
chains. The two OKX chains remain schedulable without a seed gate.

Manual workflow dispatch may select a Binance or Bybit chain before its
scheduled gate is enabled for controlled bootstrap and checkpoint validation.

Do not set a seed gate to `true` until both chains in that venue own verified L2
artifacts containing usable output checkpoints.

Local private-dataset import configuration is:

```yaml
remote:
  hf_repo_id: "maTayefi/l2shock-processed"
  hf_revision: "main"
  hf_token: ""
  default_workflow: "remote_hf_import"
```

The local read token belongs in the ignored project `.env`:

```dotenv
L2SHOCK__REMOTE__HF_TOKEN="..."
```

Use a fine-grained read token restricted to the private dataset. The local
application does not need HF write permission for imports.
Required GitHub configuration for the checked-in B2 workflow is:

```text
Actions secrets:
    L2SHOCK_B2_KEY_ID
    L2SHOCK_B2_APPLICATION_KEY

Actions variables:
    L2SHOCK_B2_ENDPOINT_URL
    L2SHOCK_B2_BUCKET

Operational Actions variables:
    L2SHOCK_DEPTH_LOWER
    L2SHOCK_DEPTH_UPPER
    L2SHOCK_CATCH_UP_HOURS
    L2SHOCK_MAX_HOURS_PER_RUN
    L2SHOCK_MAX_RUNTIME_MINUTES
    L2SHOCK_REMOTE_PROCESSING_ENABLED
    L2SHOCK_BINANCE_SEEDS_READY
    L2SHOCK_BYBIT_SEEDS_READY

Retained explicit HF rollback/source configuration:
    HF_TOKEN
    L2SHOCK_HF_REPO_ID
    L2SHOCK_HF_REVISION
```

The workflow maps the B2 Actions secrets to
L2SHOCK__REMOTE__B2__KEY_ID and
L2SHOCK__REMOTE__B2__APPLICATION_KEY in the worker process.

Production depth variables must match the exact migrated component presets.
Seed gates refer to verified checkpoints in the selected B2 destination,
not merely to earlier HF readiness.

The workflow serializes each venue/instrument chain independently while
allowing different chains to run concurrently.

The current default remote frontier-search bound is 720 hours. This is separate
from the processing budget: `L2SHOCK_MAX_HOURS_PER_RUN` still limits how many
hours one chain may process during one workflow job.

Do not enable scheduled Binance processing until the Binance snapshot
normalization path and both initialization paths have passed their verified
remote-worker tests.

A predecessor seed is required for an update-only target hour, but is not
required when the target archive contains a proven complete opening snapshot.

### Bounded multi-hour catch-up

When `--hour` is omitted, the remote worker performs bounded sequential
catch-up for exactly one venue/instrument chain.

The worker first discovers the newest safe target using one bounded Hugging
Face frontier scan. It then processes contiguous hours in ascending order.

Each dependent update-only hour is admitted only after the preceding hour
completed successfully. `process_remote_hour` resolves current Hugging Face
state for each target and establishes target initialization from either a
proven complete opening snapshot or the immediately preceding verified
checkpoint/carry state before publishing that hour independently.

Catch-up stops when:

- the newest release-eligible hour completes;
- `--max-hours-per-run` operations complete;
- the cooperative `--max-runtime-minutes` budget expires;
- the selected source is unavailable;
- checkpoint continuity is blocked;
- acquisition, processing, verification, or publication fails;
- the process is interrupted.

The runtime budget does not cancel an already-started hour. It prevents another
hour from starting after the budget expires. This avoids interrupting an atomic
Hugging Face publication solely to enforce a soft catch-up deadline.

An explicit `--hour` continues to process or verify only that exact hour.

When the scheduled worker omits an explicit target hour, it performs a bounded
newest-to-oldest inspection of one pinned Hugging Face revision.

For each venue/instrument chain:

```text
newest existing verified L2 artifact
    ->
verify usable output checkpoint
    ->
repair missing same-hour Binance price when required
    ->
process only the immediately following L2 hour
```

If the newest release-eligible hour is already complete, the worker performs an
idempotent existing-artifact verification and publishes nothing.

For Binance, catch-up fails closed when the target archive cannot establish a
complete opening state and no verified immediately preceding checkpoint or
carried reconstructed state is available inside the allowed continuity chain.

The worker never treats the first `update` event in an update-only Binance
archive as a snapshot.

OKX may select the oldest hour in an empty bounded search window because the
approved OKX contract requires the target archive to establish its own complete
opening snapshot. Headless processing must still produce a usable output
checkpoint before publication.

GitHub Actions keeps one running and at most one pending invocation for each
venue/instrument chain:

```text
cancel-in-progress: false
queue: single
```

A running predecessor is never canceled. Independent chains retain separate
concurrency groups and may run concurrently. GitHub concurrency remains an
operational serialization layer; immutable artifact identity, pinned reads,
checkpoint validation, and optimistic Hugging Face publication remain the
authoritative correctness boundaries.

Example L2 invocation:

```powershell
python -m l2shock.remote_cli l2 `
  --venue binance_futures `
  --instrument BTCUSDT `
  --hour 2026-09-14T12:00:00Z `
  --depth-lower 0 `
  --depth-upper 0.01 `
  --input-dir data\remote-worker-input `
  --output-dir data\remote-worker-output
```

For an update-only hour, supply the immediately preceding verified checkpoint:

```powershell
python -m l2shock.remote_cli l2 `
  --venue binance_futures `
  --instrument BTCUSDT `
  --hour 2026-09-14T13:00:00Z `
  --depth-lower 0 `
  --depth-upper 0.01 `
  --checkpoint-in data\previous-output.l2checkpoint `
  --input-dir data\remote-worker-input `
  --output-dir data\remote-worker-output
```

The current transport embeds the output checkpoint inside the L2 artifact.
The separate `--checkpoint-in` file is only an explicit CLI input. A later
Hugging Face adapter will extract predecessor checkpoint bytes from the
verified predecessor artifact rather than trusting an unrelated file.

Example price invocation:

```powershell
python -m l2shock.remote_cli price `
  --venue binance_futures `
  --instrument BTCUSDT `
  --hour 2026-09-14T12:00:00Z `
  --input-dir data\remote-worker-input `
  --output-dir data\remote-worker-output
```

Price processing uses only offset `0` by default. Adjacent source selection must
be explicit:

```powershell
python -m l2shock.remote_cli price `
  --venue binance_futures `
  --instrument BTCUSDT `
  --hour 2026-09-14T12:00:00Z `
  --source-offset=-1 `
  --source-offset=0 `
  --source-offset=1 `
  --input-dir data\remote-worker-input `
  --output-dir data\remote-worker-output
```

A neighboring source archive becoming available later must not silently change
the source selection of an already-defined price artifact.

CryptoHFTData hourly source files are expected approximately 15 minutes after
the source hour closes.

For example:

```text
source hour:
    [12:00:00, 13:00:00) UTC

expected publication:
    approximately 13:15 UTC
```

Remote processing may start only after the configured release delay has
elapsed. A missing source at the first attempt remains retryable.

The remote processing scope is:

```text
Binance Futures BTCUSDT orderbook
Binance Futures BTCUSDT trades
Binance Futures ETHUSDT orderbook
Binance Futures ETHUSDT trades
Bybit BTCUSDT orderbook
Bybit ETHUSDT orderbook
OKX Futures BTC-USDT-SWAP orderbook
OKX Futures ETH-USDT-SWAP orderbook
```

Bitget remains postponed. It must not be enabled until its sequence,
snapshot/bootstrap, replay, processing, publication, and import contracts are
proven independently.

The GitHub source repository may be public, but the Hugging Face dataset remains
private initially.

No secret may be committed to the public repository. Hugging Face write tokens
and any future authenticated CryptoHFTData credentials must be supplied through
repository secrets or environment-backed secret configuration.

Raw CryptoHFTData archives are temporary remote-worker inputs:

```text
create isolated temporary worker workspace
-> download through the shared CryptoHFTDownloader
-> validate Parquet structure
-> calculate source SHA-256
-> process through the shared headless boundary
-> publish and verify compact output
-> close HTTP resources
-> discard the complete temporary workspace
```

Raw archives must not be committed to Git, uploaded as long-term GitHub Actions
artifacts, or copied into the Hugging Face processed dataset.

The deterministic remote artifact unit is:

```text
artifact kind
provider
venue
instrument
completed UTC hour
component preset hash, for L2
```

Remote L2 artifacts retain authoritative compact:

```text
Bid Liquidity
Ask Liquidity
validity
invalid reason
source count
```

Total Liquidity and Bid-Ask Imbalance remain derived from Bid and Ask. They do
not become independent authoritative remote channels.

Every remote artifact must retain:

source archive SHA-256
analytical content SHA-256
input checkpoint SHA-256, when used
output checkpoint SHA-256, when available
component preset hash, for L2
complete canonical component preset content, for L2
software version
producer Git commit, when available

The L2 preset hash alone is not sufficient for a clean local import. A new
installation may not yet have the corresponding `data_presets` row.

Therefore an L2 remote manifest owns both:

```text
preset_hash
complete canonical LiquidityDataPreset content
```

The importer must reconstruct the typed preset, recompute its hash, verify that
the hash matches the remote artifact key, and then use the existing idempotent
preset repository boundary.

The remote artifact must not invent a reduced or UI-shaped preset object.

The analytical content SHA-256 is distinct from the transport Parquet file
SHA-256. A future non-semantic container migration may change transport bytes
without changing the authoritative analytical content identity.

Consecutive order-book hours for one venue/instrument chain are sequential:

```text
hour N output checkpoint
    ->
hour N+1 input checkpoint
```

Independent venue/instrument chains may run concurrently. Dependent hours in
one chain must not be published out of order.

If one hour fails, later dependent hours wait. A failed or partial hour must not
publish an authoritative output checkpoint.

The current local PostgreSQL database remains local. GitHub Actions does not
connect to the user's PostgreSQL database, and NiceGUI is not started on the
remote worker.

Raw L2 update rows are never imported into PostgreSQL.

## Current sample findings

The originally inspected Binance Futures files contain:

| Asset | Rows | Event groups | Snapshots | In-hour continuity mismatches |
|---|---:|---:|---:|---:|
| BTCUSDT | 5,451,975 | 135,002 | 0 | 0 / 135,001 |
| ETHUSDT | 4,733,525 | 134,880 | 0 | 0 / 134,879 |

Those specific inspected files contain only `update` events. They are internally
continuous but cannot initialize a complete book by themselves.

A later CryptoHFTData Binance archive was observed to contain a complete opening
`snapshot` event. Therefore Binance is not universally update-only: the
initialization method depends on the actual source hour.

For observed Binance snapshot rows, `transaction_time` and `order_count` may be
null.

For Binance snapshot rows only, the shared ingestion path uses:

```text
transaction_time = event_time when transaction_time is null
```

Null `order_count` remains null because the provider did not supply an explicit
order count.

With that in-memory normalization applied, a proven complete Binance opening
snapshot can initialize the order book without a predecessor checkpoint.

For an update-only Binance hour, valid reconstruction still requires a verified
preceding checkpoint or carried reconstructed state. The first event in a file
must never be treated as a snapshot merely because it is the first event.

---

## Multi-venue empirical findings

A later inspection of CryptoHFTData archives for 2026-09-09 showed that
normalized field presence and sequence ownership vary by venue even when the
physical Parquet column names are shared.

Observed examples include:

```text
OKX Futures:
    venue path = okx_futures
    BTC instrument = BTC-USDT-SWAP
    ETH instrument = ETH-USDT-SWAP
    opening snapshot rows observed
    transaction_time may be null

Bybit:
    venue path = bybit
    BTC/ETH instruments use BTCUSDT / ETHUSDT
    sampled hours were update-only
    first_update_id and prev_final_update_id may be null
    final_update_id and last_update_id have distinct meanings

Bitget Futures:
    venue path = bitget_futures
    BTC/ETH instruments use BTCUSDT / ETHUSDT
    sampled hours were update-only
    transaction_time, first_update_id, prev_final_update_id, and last_update_id
    may be null
```

The shared column names do not prove shared reconstruction semantics.

The production Binance reader/replay contract must not be reused for another
venue merely because the Parquet schema contains the same columns.

Before enabling a venue, the application must establish:

```text
snapshot representation
event grouping identity
authoritative sequence frontier
update continuity relation
cross-hour behavior
quantity unit
linear/inverse notional convention
instrument identity
timestamp nullability and units
```

The 2026-09-09 OKX BTC and ETH samples were subsequently validated through the
strict OKX adapter.

Two adjacent hours for each base replayed successfully with:

```text
opening 800-level snapshot per hour
400 bid levels
400 ask levels
zero replay invalidations
valid final checkpoint
```

The production reader accepts OKX's nullable `transaction_time`, and the replay
engine applies its empirically established `last_update_id` predecessor
contract.

This does not make the shared physical Parquet schema a universal sequence
contract. Any future venue still requires an explicit adapter and empirical
replay tests. Venue support must never be added merely by widening an
allowed-name list.

### OKX Futures sequence adapter

Empirical inspection of adjacent OKX Futures BTC and ETH archives for
2026-09-09 established the first non-Binance sequence adapter.

Observed normalized ownership is:

```text
transaction_time:
    null

first_update_id:
    null

prev_final_update_id:
    null

snapshot:
    final_update_id == last_update_id
    resulting frontier = final_update_id

update:
    predecessor frontier = last_update_id
    resulting frontier = final_update_id
```

Update continuity therefore requires:

```text
current.last_update_id
==
previous resulting frontier
```

It does not require adjacent `final_update_id` values. OKX final update IDs may
advance by more than one event-internal unit.

Each inspected UTC hour begins with a complete 800-level-row snapshot. An
opening snapshot resets any carried state, even if a checkpoint from the
preceding hour was initially available.

This adapter is strict and venue-specific. It does not alter the Binance
Futures contract.

Bybit now has a strict isolated replay adapter and is included in local
production acquisition, local processing, single-market preset management,
Automatic Fetch completeness, and local source availability.

Bybit is included in the GitHub remote-worker matrix and private Hugging Face
publication path behind its independent scheduled seed gate.

Bybit is included in local Hugging Face range-import planning. A selected base
and hour imports four exact remote artifacts:

```text
Binance component L2
Bybit component L2
OKX component L2
Binance real-trade price
```

Local Analysis supports an approved Binance + Bybit + OKX aggregate preset.
The aggregate resolves all three independently persisted component preset
hashes and performs strict exact-second aggregation at verified Analysis load
time.

Bitget remains disabled for production replay until a separate adapter is
established:

```text
Bybit:
    venue path = bybit
    BTC/ETH instruments = BTCUSDT / ETHUSDT
    first_update_id and prev_final_update_id are null in inspected streams
    order_count is null in inspected streams
    final_update_id is the update replay frontier
    adjacent update final_update_id values increment by exactly one
    the same final_update_id + 1 relation crosses inspected UTC-hour boundaries
    last_update_id is a separate increasing sequence/ordering identity
    complete 50-bid / 50-ask snapshot events occur inside archived hours
    native snapshots with final_update_id initialize that replay frontier
    archive-boundary snapshots may omit final_update_id and transaction_time
    a frontier-less boundary snapshot may replace only valid carried state
    a frontier-less snapshot cannot independently initialize replay

Bitget:
    first_update_id, prev_final_update_id, last_update_id, and
    transaction_time may be null
    observed final_update_id progression does not establish a complete
    continuity contract
```

### Bybit diagnostic evidence and regression workflow

Bybit production replay is enabled through its isolated strict adapter. The
dedicated GitHub Actions diagnostic remains available as a regression and
format-drift detector.

The diagnostic workflow is:

```text
.github/workflows/bybit-contract-diagnostics.yml
```

It downloads two adjacent Bybit order-book archives for BTCUSDT or ETHUSDT and
records:

```text
physical row count
logical event count
snapshot row count
snapshot event count
positive bid and ask rows per snapshot
duplicate side/price identities inside an event
nullable sequence-field patterns
received-time regressions
adjacent final_update_id relationships
adjacent last_update_id relationships
cross-hour final_update_id relationships
cross-hour last_update_id relationships
bounded first, last, update, and snapshot event samples
```

A diagnostic snapshot is considered only a complete-initialization candidate
when one logical snapshot event contains:

```text
at least one positive bid
at least one positive ask
no malformed quantity
no duplicate side/price identity
```

The initial diagnostic classification was evidence only. The dedicated Bybit
sequence adapter now uses `final_update_id` as its replay frontier and requires
every update to satisfy:

```text
current.final_update_id == previous replay frontier + 1
```

`last_update_id` remains a distinct provider sequence identity and is not used
as the replay frontier.

A Bybit snapshot with a non-null `final_update_id` replaces the local book and
sets the replay frontier to that value.

A complete archive-boundary snapshot with null `final_update_id` may replace
book levels only when replay already owns a valid carried/checkpoint frontier.
In that case the existing frontier is preserved. Without carried state, such a
snapshot is rejected rather than deriving a frontier from a later update.

Official Bybit terminology distinguishes:

```text
u:
    update ID

seq:
    cross-stream sequence number
```

CryptoHFTData exposes normalized field names. The application must not assume
that `final_update_id` and `last_update_id` correspond to `u` and `seq`, or
that either field is the strict predecessor frontier, until the normalized
mapping and adjacent-event/cross-hour behavior are proven from the downloaded
archives.

The GitHub diagnostic prints the complete bounded report into the Actions log
and uploads the JSON report. Raw Bybit source archives are temporary runner
inputs and are not uploaded as workflow artifacts.

### Local Bybit production integration

The normal local production source universe now includes:

```text
Bybit BTCUSDT orderbook
Bybit ETHUSDT orderbook
```

Bybit is independently:

```text
downloaded
sequence-validated
reconstructed
one-second sampled
depth-band calculated
encoded
persisted
```

under its own single-market preset hash.

Bybit does not contribute price OHLC. Binance Futures trades remain the sole
price source.

The local production acquisition universe now contains eight source archives
per exact UTC hour:

```text
Binance Futures BTCUSDT orderbook
Binance Futures BTCUSDT trades
Binance Futures ETHUSDT orderbook
Binance Futures ETHUSDT trades
OKX Futures BTC-USDT-SWAP orderbook
OKX Futures ETH-USDT-SWAP orderbook
Bybit BTCUSDT orderbook
Bybit ETHUSDT orderbook
```

Bybit is included in the GitHub remote-worker matrix, Hugging Face publication
path, and local pinned-revision Hugging Face range-import planner.

The Binance + Bybit + OKX aggregate preset resolves independently persisted
Binance, Bybit, and OKX component rows at Analysis load time.

### OKX production integration

The normal local production source universe now includes, per exact UTC hour:

```text
Binance Futures BTCUSDT orderbook
Binance Futures BTCUSDT trades
Binance Futures ETHUSDT orderbook
Binance Futures ETHUSDT trades
OKX Futures BTC-USDT-SWAP orderbook
OKX Futures ETH-USDT-SWAP orderbook
Bybit BTCUSDT orderbook
Bybit ETHUSDT orderbook
```

Binance Futures trades remain the sole price source for optional chart OHLC
context.

OKX trade files are not included in normal production acquisition merely
because their schema is available. The OKX integration currently contributes
single-market reconstructed L2 liquidity only.

Manual Fetch and Automatic Fetch use the same eight-file local source universe.

Downloaded processing routes order-book targets through exact venue-specific
single-market presets:

```text
binance_futures:
    BTCUSDT / ETHUSDT

bybit:
    BTCUSDT / ETHUSDT

okx_futures:
    BTC-USDT-SWAP / ETH-USDT-SWAP
```

Each market remains independently reconstructed, validated, sampled, encoded,
and persisted under its own deterministic preset hash.

Binance and OKX remain independently persisted under their component
single-market preset hashes.

For an approved Binance+OKX aggregate preset, verified Analysis loading resolves
those component hashes and performs exact UTC-second-aligned aggregation in
memory.

The aggregate is derived at Analysis load time and is not written as a separate
`l2_hourly_series` row.


### Binance and OKX aggregate-liquidity identity

The first approved multi-market liquidity preset contains:

```text
CryptoHFTData Binance Futures
    BTCUSDT or ETHUSDT

CryptoHFTData OKX Futures
    BTC-USDT-SWAP or ETH-USDT-SWAP
```

Each market remains independently:

```text
downloaded
reconstructed
sequence-validated
one-second sampled
depth-band calculated
encoded
persisted
verified on read
```

The aggregate preset does not own a new `l2_hourly_series` storage row.

Its component single-market preset hashes are derived deterministically from:

```text
aggregate preset depth band
aggregate preset base
each eligible-market identity
existing schema and algorithm versions
```

Analysis-time aggregation aligns component rows by exact UTC second and sums:

```text
aggregate Bid Liquidity =
    sum(Bid Liquidity from valid contributing markets)

aggregate Ask Liquidity =
    sum(Ask Liquidity from valid contributing markets)
```

Aggregate one-second quality is:

```text
VALID:
    every expected market contributes valid Bid and Ask Liquidity

DEGRADED:
    at least one, but fewer than all, expected markets contribute

INVALID:
    no expected market contributes
```

A missing or invalid component market is never forward-filled and never
replaced with zero.

The retained aggregation foundation represents a degraded aggregate as
the exact sum of the valid markets that contributed at that UTC second.

Detector-free Analysis renders those partial-market sums. Partial
coverage remains explicitly counted and warned about; it is not labelled
complete-market coverage. Missing markets contribute no observation,
rather than a fabricated zero or a forward-filled value.

A viewing bar remains renderable when at least one numerical L2 second
contributes. Changes in contributing markets can create apparent
liquidity changes and must be considered during manual interpretation.

The aggregate coverage records:

```text
expected_market_count
contributing_market_count
per-market component preset hash
per-hour component L2 content SHA-256
```

Aggregate source count equals the number of contributing markets.

Bid-Ask Imbalance is derived from aggregate Bid and Ask Liquidity. When their
sum is zero, imbalance is invalid even though the aggregate Bid and Ask values
themselves may remain valid.

Cross-market overlap and aggregation never merge raw updates, replay
frontiers, snapshots, or checkpoints.


### Binance, Bybit, and OKX aggregate-liquidity identity

The approved three-market aggregate preset contains:

```text
CryptoHFTData Binance Futures:
    BTCUSDT or ETHUSDT

CryptoHFTData Bybit:
    BTCUSDT or ETHUSDT

CryptoHFTData OKX Futures:
    BTC-USDT-SWAP or ETH-USDT-SWAP
```

Each market remains independently reconstructed and persisted under its exact
single-market component preset hash.

The three-market aggregate owns no separately persisted aggregate L2 row.
Verified Analysis loading derives its component hashes, loads each component
through the ordinary codec and provenance verification boundary, and sums
exact Bid and Ask Liquidity only after all components are verified.

An Analysis second is numerically usable when at least one expected market
contributes valid Bid and Ask Liquidity at that exact UTC second.
Missing Bybit, Binance, or OKX observations are never zero-filled or
forward-filled.

If only some expected markets contribute, Analysis records a partial-market
second and renders the exact sum of those valid contributors. If no market
contributes, the second remains numerically unavailable.

A viewing bar remains renderable when at least one numerical L2 second
contributes. Coverage gaps do not abort the complete Analysis request and
do not suppress available candles.

Partial-market sums are plotted with explicit quality warnings. Changes
in the contributing venue set can create apparent Bid, Ask, Total, Delta,
percentage, or change-metric movements. These are research observations,
not proof of a complete-market liquidity change.

### Warning-only aggregate Analysis coverage

Multi-market presets are resolved into their immutable component single-market
preset identities during verified Analysis loading.

Each available component row is loaded and decoded independently through the
normal single-market PostgreSQL verification boundary.

At every exact UTC second, Analysis sums valid Bid and Ask observations from
the expected markets that actually contribute. Complete-market and
partial-market coverage remain distinguishable.

A missing or invalid expected market is never represented by a fabricated
zero and is never forward-filled. Its absence remains a coverage fact.

Partial-market sums are plotted with explicit quality warnings. A viewing
bar remains renderable when at least one numerical L2 second contributes.
A bar with no numerical observations remains null.

Quality gaps do not abort the complete Analysis request or suppress available
candles. Source identity, codec, hash, preset, and provenance verification
remain mandatory.

A change from Binance-only to Binance+OKX coverage can create an apparent
liquidity step. Analysis deliberately exposes the available data and reports
the coverage limitation rather than hiding the entire interval. Cross-check
price and L2 with trdr.io before trading.

Aggregate coverage provenance records:

```text
aggregate preset hash
expected market identities
component single-market preset hashes
per-hour component content SHA-256 values
available and expected market counts
```

The aggregate preset itself does not own a new persisted L2 block. Existing
single-market compact rows remain the authoritative storage layer.

Changing component availability changes the deterministic Analysis identity.
It does not rewrite any persisted source or analytical row.



## Architecture Contract for Humans and AI Reviewers

> **Purpose:** This section is an architecture contract, not ordinary user
> documentation. Its primary purpose is to stop future maintainers and AI models
> from unintentionally changing intentional but non-obvious behavior.
>
> When deciding whether information belongs here, ask:
>
> **“If a future maintainer reads this README without reading the entire
> codebase, would this information materially reduce the chance of an incorrect
> code change?”**
>
> If yes, preserve it here.

### Source authority

Current repository files are authoritative.

Previous conversations, generated patches, snippets, proposals, and
implementation discussions are historical context only. Never reconstruct a
current file from an older snippet when the current file is available.

### Documentation transport and character normalization

This README may be copied through external AI systems, sanitizers, exporters,
or chat interfaces that normalize or remove Unicode punctuation and symbols.

When reviewing the README:

- Do not classify Unicode-to-ASCII normalization as a semantic or structural
  defect.
- The following representations are semantically equivalent for review:
  - `->` and `→`
  - `*` and `×` when used as multiplication
  - `x` and `×` when used as a separator
  - `0.35-0.40` and `0.35–0.40`
- Do not propose restoring Unicode symbols merely because an ASCII
  representation appears in a copied/exported version.
- Treat the repository file on disk as authoritative when determining whether
  a character is actually missing or malformed.
- Only report character changes when they alter Markdown structure, code
  syntax, a literal protocol value, a filename/path, an identifier, or
  mathematical meaning.

For maximum cross-system portability, new architecture-contract examples
should prefer ASCII-only notation unless a Unicode character is itself
significant.

AI review rule: Do not propose README character or formatting repairs for
Unicode-to-ASCII normalization unless the change affects Markdown structure,
code syntax, a literal protocol value, a filename/path, an identifier, or
mathematical meaning.

### Change classification

Every proposed change must be classified as either:

- `SEMANTIC`
- `NON-SEMANTIC`

A semantic change includes any change to:

- order-book initialization;
- event grouping or ordering;
- sequence validation;
- market eligibility;
- notional conversion;
- liquidity depth calculation;
- bucket sampling;
- data-quality interpretation;
- gap and invalid-second handling;
- expected-market composition and partial-market handling;
- mathematical metric formulas and undefined-denominator policy;
- which observations contribute to viewing-bar OHLC;
- change-metric predecessor ownership;
- outage measurement ranges and threshold comparisons.

Semantic changes must never be made silently.

Non-semantic changes include implementation, performance, UI, logging,
refactoring, cancellation, error handling, testing, and storage optimization
only when analytical output remains mathematically equivalent.

Persisted-data semantic changes require the appropriate algorithm, preset,
schema, or codec version change. Display-only settings remain outside
persisted data-preset identity, but changing their mathematics or gap policy
still requires explicit semantic review.

Detector-specific scales, hypotheses, evidence, ranking, and A/B/C annotations
are retired functionality, not extension points in the Analysis workflow.

### Raw archive and PostgreSQL boundary

Raw CryptoHFTData L2 rows remain in local Parquet files.

Raw updates must not be copied into PostgreSQL. One inspected BTC hour contains
more than 5.4 million rows, making ordinary raw-row PostgreSQL storage
inappropriate for the supported machine and retention target.

PostgreSQL stores compact derived observations, metadata, provenance, quality
state, preset identity, and operational history.

### CryptoHFTData acquisition transport

CryptoHFTData remains the external historical-data source for this project.

Version 1 uses:

```text
Primary unattended transport:
    direct REST hourly-file downloads through httpx

Local archival format:
    original hourly Parquet files

Production Parquet processing:
    streamed PyArrow row groups / record batches
```
#### REST archive contract

The production REST base URL is:

```text
https://api.cryptohftdata.com/v1
```

Hourly files are downloaded using:

```http
GET /download?file=<complete-hourly-archive-path>
```

The required query parameter is named exactly:

```text
file
```

The normalized hourly archive path is:

```text
{exchange}/{YYYY-MM-DD}/{HH}/{symbol}_{data_kind}.parquet
```

Confirmed Binance Futures examples are:

```text
binance_futures/2026-09-02/12/BTCUSDT_orderbook.parquet
binance_futures/2026-09-02/12/BTCUSDT_trades.parquet
```

Anonymous downloads omit credentials and use the documented rate-limited free
tier.

Direct API-key authentication may use the optional `api_key` query parameter.
JWT authentication may instead use:

```http
Authorization: Bearer <JWT_TOKEN>
```

JWT acquisition uses the separately documented `/jwt-token` flow. Credentials
must never appear in logs, exception messages, provenance, database rows, or
committed configuration.

Version 1 begins with anonymous REST downloads. Optional authenticated REST
support must preserve the same source-file identity and must redact credentials
at every diagnostic boundary.

Internally, requested UTC ranges use half-open interval semantics:

```text
[start_utc, end_utc)
```

Every UTC source hour intersecting that interval is planned exactly once.
Source-file identities themselves remain aligned to exact UTC-hour boundaries.

This locks the endpoint and prevents a future implementation from inventing
path=, using the website root, or changing path layout.

The official `cryptohftdata` Python SDK is not currently a production
dependency because its convenience workflow is DataFrame-oriented, while this
application must avoid materializing multi-million-row L2 hours in pandas.

S3-compatible flat-file access remains a future optional bulk-backfill
transport. Its temporary credentials are separate from the REST API key and
must not be stored permanently or assumed to have an undocumented automatic
refresh mechanism.

Do not add the SDK or `boto3` merely because those access methods exist.
Dependencies must correspond to an implemented production transport.

CryptoHFTData Binance Futures BTCUSDT and ETHUSDT are the first reconstruction
validation targets. This is an implementation priority, not a permanent
Binance-only liquidity restriction.

The longer-term processing order remains:

```text
CryptoHFTData venue/instrument
    -> independent reconstruction
    -> continuity validation
    -> per-market liquidity
    -> valid cross-market aggregation
```

A separate Binance API data pipeline must not be introduced merely to validate
CryptoHFTData.


### Automatic-fetch catch-up policy

Automatic Fetch protects the newest release-eligible UTC source hour first.

Once that hour has all eight required local production source archives:

```text
Binance Futures BTCUSDT orderbook
Binance Futures BTCUSDT trades
Binance Futures ETHUSDT orderbook
Binance Futures ETHUSDT trades
OKX Futures BTC-USDT-SWAP orderbook
OKX Futures ETH-USDT-SWAP orderbook
Bybit BTCUSDT orderbook
Bybit ETHUSDT orderbook
```

the runtime searches backward for the nearest incomplete source hour inside:

```text
cryptohft.automatic_fetch_catch_up_hours
```

The default bounded catch-up window is:

```text
72 hours
```

Only one incomplete UTC hour is requested per polling iteration. Existing valid
local archives are reused through the normal acquisition path.

Catch-up remains acquisition-only. It does not automatically invoke L2 or price
Processing.

An hour is considered locally acquisition-complete when all eight expected
production source identities are proven by durable metadata.

Downloaded and processing rows require an intact canonical local archive whose
size matches durable metadata.

A local processed row with a non-null local path also requires that intact
canonical file. A processed row remains acquisition-complete after explicit raw
pruning when its local path is null and its durable size and content SHA-256
remain present.

A verified Hugging Face import is a separate processed-source case. It records
the canonical source SHA-256 and a typed remote-import ownership marker without
inventing a local raw path or raw size. Automatic Fetch treats that verified
remote ownership as acquisition-complete while the remote-import profile owns
normal acquisition.

If a pre-existing source row already claims a local raw path, remote import
preserves that attachment only after verifying:

```text
canonical raw-root path
regular non-symbolic-link file
durable file size
complete source SHA-256
```

An invalid claimed local attachment fails the complete import transaction. The
remote importer must not bless or preserve stale local-file metadata merely
because the verified remote analytical artifact is valid.


Raw pruning and remote import must not cause Automatic Fetch to redownload an
already processed source. Reattaching local raw bytes remains the separate
raw-rehydration workflow.

The catch-up search is bounded. Automatic Fetch does not silently start an
unlimited historical backfill.

### Automatic-fetch catch-up fairness

Automatic Fetch inspects a bounded newest-to-oldest UTC-hour window.

The first attempt for each newly release-eligible window protects the newest
incomplete hour.

After an hour has received a real coordinator attempt, the retry cursor moves
to the next older incomplete hour. Selection wraps to the newest incomplete
hour after reaching the oldest hour in the configured window.

This prevents one persistently missing or delayed recent archive from starving
every older gap.

The retry cursor is persisted in `app_settings` and is scoped to the exact
newest release-eligible UTC hour. When a new hour becomes eligible, the cursor
is ignored and the new hour receives first priority.

The cursor records attempted order only. It never marks a source hour complete.
Completeness continues to be derived from all eight required local source
identities.

Only one incomplete UTC hour is requested per polling iteration.

### Raw-retention dry-run policy

The configured raw retention duration is:

```text
storage.raw_retention_hours
```

Retention diagnostics remain read-only.

They report processed raw archives older than the configured cutoff and verify:

```text
processed source status
canonical source identity
canonical raw-root path
regular-file ownership
durable size consistency
actual file SHA-256
canonical source SHA-256 metadata
processed_at ownership
```

Explicit raw pruning is separately available through Settings maintenance.

It requires:

```text
fresh preview
explicit confirmation
candidate-set revalidation
source advisory lock
same-directory atomic rename
database local_path clearing
post-commit file unlink
structured audit result
```

Downloaded but unprocessed archives are never retention candidates.

A pruned processed row retains its durable size, SHA-256, status, quality, and
analytical provenance. Reattaching a reacquired raw archive remains a separate
future operation.


## Production diagnostics and maintenance reports

The Settings tab can build a read-only production diagnostics report.

The report inventories:

```text
configured runtime directories
file counts and encoded byte totals
free disk capacity
PostgreSQL table row counts
source-hour status counts
source-hour quality counts
enabled data presets
raw-retention dry-run candidates
blocked retention rows
estimated reclaimable raw bytes
```

Filesystem inventory does not follow symbolic links.

The raw-retention section reuses the fail-closed retention planner. It reports
only processed source archives older than the configured cutoff which still
match their canonical source identity, path, size, content-hash metadata, and
processed timestamp.

The report does not:

```text
delete raw archives
mutate source-hour metadata
delete checkpoints
delete analytical rows
change processing status
```

Production diagnostics may be exported as deterministic JSON. Diagnostic
exception arguments are not exported because database or filesystem exception
messages may contain sensitive local implementation details.

Explicit raw-file pruning is implemented through the separately confirmed
maintenance workflow.

The current limitation is raw rehydration: a pruned `processed` source row
cannot yet automatically reattach a redownloaded archive while preserving its
processed analytical identity. Rehydration must verify the reacquired bytes
against durable size and SHA-256 metadata.

A diagnostics JSON file is an operational report. It is not a database backup.


### Checkpoint, stale-state, and analytical consistency diagnostics

Production diagnostics include a read-only checkpoint inventory.

Every discovered `.l2checkpoint` artifact is checked for:

```text
regular-file ownership
canonical content-addressed path
checkpoint decoding
canonical content SHA-256
provider / venue / instrument identity
completed UTC-hour identity
durable source-hour reference
```

The report distinguishes:

```text
valid checkpoint artifacts
invalid or corrupt artifacts
unreferenced/orphan artifacts
missing checkpoints explicitly referenced by source metadata
malformed source checkpoint references
unusually large checkpoint artifacts
```

An orphan report is not deletion authorization. A checkpoint may have been
published immediately before a database transaction failed and can therefore be
a valid reusable artifact even when it is temporarily unreferenced.

Transient source rows are reported when they remain in:

```text
downloading
processing
```

beyond the diagnostic thresholds.

The current schema does not persist `processing_started_at`. Processing age
therefore uses `downloaded_at` when available and otherwise `discovered_at`.
This is a conservative queue-inclusive age, not an exact processing duration.

Processed source metadata is compared with compact analytical rows using the
exact content hashes recorded by processing:

```text
orderbook:
    source_hours.quality_json.analytical_content_sha256

trades:
    source_hours.quality_json.price_content_sha256
```

Diagnostics report:

```text
processed source rows missing expected analytical content
malformed processed source metadata
unreferenced/orphan L2 rows
unreferenced/orphan price rows
L2 base/hour identities without price
price base/hour identities without L2
unusually large compact analytical rows
```

The L2/price comparison is a base/hour availability comparison. It does not
merge markets, modify preset identity, or perform cross-market aggregation.

Fetch duration summaries are derived from durable `fetch_runs.started_at` and
`fetch_runs.ended_at` timestamps.

The current schema does not persist exact processing-operation start/end
history or peak-memory measurements. The report explicitly marks those
histories unavailable rather than inventing values.

`source_hours.downloaded_at` to `source_hours.processed_at` is reported only as
queue-inclusive source latency. It must not be labelled exact processing
duration.

All maintenance diagnostics remain read-only. They never:

```text
delete checkpoints
delete raw files
delete analytical rows
repair analytical rows
reset stale source status
change source quality
publish checkpoints
run processing
```

### Explicit destructive maintenance

Maintenance diagnostics remain read-only.

Destructive maintenance is available only through explicitly initiated
Settings actions:

```text
recover stale downloading/processing source rows
delete old unreferenced checkpoint artifacts
prune old processed raw archives
```

In version 1, “checkpoint deletion” means orphan-checkpoint deletion only.

A checkpoint referenced by committed source quality metadata is durable
continuation state and is not eligible for general user-directed deletion.
There is deliberately no separate action that deletes referenced checkpoints.

Every action requires:


```text
fresh dry-run preview
complete candidate identity
short-lived deterministic preview token
explicit user confirmation
process-wide operation lock
shutdown admission check
final candidate-set revalidation
structured audit result
```

If the candidate set changes between preview and execution, execution is
refused and a new preview is required.

Stale-source recovery is fail-closed:

```text
stale downloading:
    error

stale processing with intact replayable raw archive:
    downloaded

stale processing without intact replayable raw archive:
    error
```

Returning `processing` to `downloaded` uses the existing explicit cooperative
cancellation-reset policy. It is never treated as an ordinary status
transition.

Orphan checkpoint deletion is restricted to canonical, valid checkpoint
artifacts which:

```text
are under the configured checkpoint root
match their content-addressed path
are not referenced by source quality metadata
are older than the configured minimum orphan age
```

Invalid, noncanonical, symbolic-link, young, or referenced artifacts are never
deleted automatically.

Raw-file pruning uses the existing processed-retention plan. Before mutation it
revalidates:

```text
processed source status
canonical source identity and path
regular-file ownership
durable size metadata
durable content SHA-256 metadata
retention cutoff
```

The file is first renamed atomically inside its source directory. The database
then clears only `source_hours.local_path`. After the transaction commits, the
renamed file is unlinked.

Raw pruning preserves:

```text
processed status
file_size_bytes
content_sha256
quality state
quality diagnostics
analytical rows
analytical provenance
```

If database mutation fails, the renamed file is restored when possible.

Maintenance never deletes or repairs:

```text
l2_hourly_series
price_hourly_series
data-preset references
analytical provenance
```

Automatic destructive maintenance remains disabled.



### Hourly availability calendar

The Fetch tab displays availability by exact CryptoHFTData UTC source hour.

Source-hour identities are never converted into local-hour storage buckets.
Instead, each exact UTC source hour is labelled using the configured local
timezone.

This distinction matters for timezones whose offset is not a whole hour, such
as:

```text
Asia/Tehran = UTC+03:30
```

For one selected local civil date, the application:

```text
converts local midnight boundaries strictly to UTC
-> expands to enclosing exact UTC source-hour bounds
-> loads source and analytical availability
-> retains source hours whose local start belongs to that date
```

Availability states are:

```text
UNKNOWN:
    no source or analytical record is known

MISSING:
    acquisition explicitly recorded a missing remote source and no stronger
    local or analytical evidence exists

PARTIAL:
    only part of the required source or analytical material exists

DOWNLOADED:
    both order-book and trade archives are locally available

MATERIALIZED:
    at least one expected component compact L2 row and the compact real-trade
    price row exist, but no expected L2 component or the price side has a valid
    one-second observation

ANALYZABLE:
    the compact real-trade price row has at least one valid one-second candle
    and at least one expected L2 component market has a valid one-second
    observation
```

For a multi-market preset, calendar availability resolves the aggregate
preset into its deterministic component single-market preset hashes.

The calendar reports:

```text
expected L2 market count
materialized L2 market count
valid L2 market count
```

A multi-market aggregate hour is `ANALYZABLE` only when every expected market
is materialized and valid for all 3,600 L2 seconds. Partial market coverage
remains visible diagnostically but is not eligible for Analysis handoff. The UI labels the
coverage as partial rather than treating the missing market as zero.

Calendar handoff retains a conservative full-market coverage criterion for
its analyzable-window shortcut. This is not an Analysis rendering requirement:
a manually entered Analysis range can render available verified observations,
including partial-market sums, with quality warnings.

The newest contiguous analyzable window is calculated independently for BTC
and ETH using each base's newest enabled data preset.

A calendar handoff transfers:

```text
base
preset hash
contiguous UTC start
contiguous UTC exclusive end
hour count
```

into the Analysis controls.

Because Analysis controls use closed user endpoints while storage ranges are
half-open, the handoff converts:

```text
[start_hour, exclusive_end_hour)
```

to:

```text
closed Analysis endpoints:
start_hour
exclusive_end_hour - 1 second
```

This includes the final one-second analytical bar without including the next
source hour.

The calendar refreshes after:

```text
manual Fetch completion
automatic Fetch polling completion
manual Processing completion
manual refresh or date navigation
```

Automatic fetching still does not automatically process downloaded files.
Fetch and Processing remain separately admitted operations.

### Event grouping

One exchange update event may contain many price-level rows.

Rows belong to the same event only when their normalized event identity agrees,
including the relevant sequence fields and symbol. Sequence continuity is
validated between event groups, not between individual price-level rows.

### Snapshot, checkpoint, and replay requirement

An update-only file does not establish a complete order book.

A file containing a proven complete opening `snapshot` may initialize the book
independently. No liquidity observation may be marked valid before the book is
initialized from either:

- a proven complete snapshot;
- a verified carried state from the immediately preceding hour; or
- a verified checkpoint from the immediately preceding hour.

A replay checkpoint belongs to one exact:

```text
provider
venue
instrument
completed UTC source hour
final update ID
complete bid/ask level state
```

A checkpoint may initialize only the immediately following UTC source hour for
the same provider, venue, and instrument. It must not be applied across a
missing hour or to another source identity.

For the currently observed Binance Futures normalized update contract,
cross-event and cross-hour continuity requires:

```text
current.prev_final_update_id
==
previous_valid_final_update_id
```

The implementation does not require:

```text
current.first_update_id
==
previous_valid_final_update_id + 1
```

because that stricter relation has not been established by the real inspected
archives.

Quantity zero removes the identified side/price level.

A complete snapshot clears prior local state and initializes a replacement
state only when it contains:

```text
last_update_id
at least one positive bid level
at least one positive ask level
```

If continuity fails, an update regresses, or a conflicting replayed event is
observed, the complete local book state is discarded. Later updates remain
unapplied until a newer valid complete snapshot/checkpoint establishes state.

An exact immediately repeated update may be skipped only when its complete
normalized event identity and level changes match the immediately preceding
applied update. Reuse of an update frontier with conflicting content invalidates
the state.

A replay report proving within-hour continuity does not by itself prove that an
update-only hour was initialized. A final checkpoint exists only when replay
finishes with a complete valid book.

A live exchange snapshot must never be assigned to a historical source hour.
For example, a Binance REST depth snapshot captured now cannot be labelled as
the state at the end of an earlier CryptoHFTData hour.

Valid historical initialization requires one of:

```text
a complete snapshot contained in the historical source archive
a verified checkpoint produced by replay of the immediately preceding hour
a verified carried state from that immediately preceding hour
```

Synthetic historical checkpoints built from current live API state are
prohibited.

### Binance snapshot-field normalization

CryptoHFTData Binance opening snapshot rows may contain a null
`transaction_time` even though Binance update events require that field.

For Binance `snapshot` rows only, the shared ingestion layer applies:

```text
transaction_time:
    use event_time when transaction_time is null
```

Binance update rows retain the strict non-null `transaction_time` requirement.

A null snapshot `order_count` remains null. Unknown order-count information is
not fabricated as zero.

This is a provider-compatibility normalization performed in memory while
streaming. The original CryptoHFTData Parquet file remains unchanged, so source
archive SHA-256 provenance continues to identify the exact downloaded bytes.


### Serialized checkpoint contract

A serialized replay checkpoint is a versioned binary artifact containing:

```text
binary format magic and version
canonical payload byte length
canonical payload SHA-256
provider / venue / instrument
completed UTC source hour
final update ID
complete positive bid and ask levels
optional source-archive SHA-256
```

Checkpoint price and quantity values remain exact decimal strings in the
canonical payload. Floating-point checkpoint level identity is prohibited.

Canonical level ordering is:

```text
bids: descending price
asks: ascending price
```

Encoding the same mathematical checkpoint state must produce the same bytes
regardless of the input tuple order of its levels.

Checkpoint decoding is bounded before parsing. It rejects:

```text
unknown format versions
oversized declared payloads
truncation
trailing bytes
payload hash mismatches
duplicate JSON keys
unexpected or missing fields
noncanonical decimal text
noncanonical level ordering
duplicate side/price identities
missing bid or ask state
invalid source identity
invalid UTC-hour identity
```

The embedded SHA-256 detects accidental corruption. It is not a digital
signature and does not establish that the source data or replay process was
trustworthy.

A decoded checkpoint remains subject to the same exact source identity and
immediately-following-hour ownership checks as an in-memory checkpoint.

Checkpoint files are published atomically. Existing checkpoint destinations are
not overwritten unless the caller explicitly requests replacement.

Serialized checkpoints must never contain credentials, API keys, database
passwords, UI settings, or unrelated application configuration.

### Replay validation utility

Adjacent local order-book archives may be inspected with:

```text
py -3.14 -m l2shock.ingest.replay_validation
    --symbol BTCUSDT
    --archive 2026-09-02T12:00:00Z <hour-12-path>
    --archive 2026-09-02T13:00:00Z <hour-13-path>
    --json-out replay-report.json
    --checkpoint-out BTCUSDT-13.l2checkpoint
```

Archive arguments must be supplied in strict adjacent UTC-hour order.

The utility may accept a checkpoint from the immediately preceding hour:

```text
--checkpoint-in <checkpoint-path>
```

It never treats the first update in an update-only archive as a snapshot.

Exit statuses are:

```text
0:
    replay completed with a usable final checkpoint

2:
    invalid input, source, checkpoint, or replay contract

3:
    replay completed but no usable final checkpoint exists

130:
    cancelled or interrupted
```

A structurally completed replay report is not automatically proof of analytical
liquidity validity. In particular, crossed, locked, and empty-side book states
remain explicit diagnostics for later quality policy.


### Production checkpoint store

Production checkpoints use the deterministic content-addressed layout:

```text
data/cache/checkpoints/
    {provider}/
        {venue}/
            {instrument}/
                {YYYY-MM-DD}/
                    {HH}/
                        checkpoint-v1-{content_sha256}.l2checkpoint
```

The SHA-256 in the filename covers the complete encoded checkpoint artifact.

For one exact provider, venue, instrument, and completed UTC source hour:

- publishing identical content is idempotent;
- different content is a conflict;
- corrupted or incorrectly located artifacts fail closed;
- checkpoint discovery is bounded by
  `processing.checkpoint_search_max_hours`;
- discovery cannot cross a missing source hour;
- reaching the search bound without a checkpoint is not itself an error,
  because a source archive in the returned contiguous chain may contain a
  valid snapshot.

The production single-market L2 coordinator performs:

```text
checkpoint and contiguous-source discovery
-> exact source SHA-256 verification
-> predecessor replay for initialization
-> target-hour one-second liquidity sampling
-> compact block encoding
-> immutable checkpoint publication
-> idempotent PostgreSQL persistence
-> source-hour terminal metadata
```

Predecessor hours are replayed only to establish target initialization. Their
3,600 depth-liquidity slots are not recalculated merely to process one later
target hour.

Checkpoint publication is the cancellation commit point for L2 processing.
Cancellation is honored before publication. Once publication begins, the
coordinator completes the short idempotent persistence/finalization path rather
than falsely reporting a safe cancellation reset.

A cooperatively cancelled source may transition from `processing` back to
`downloaded` only when the caller explicitly proves that no terminal analytical
mutation was committed.


### Replay book-structure diagnostics

After each applied snapshot or update, replay records one of:

```text
normal
locked
crossed
empty_bid
empty_ask
empty_both
```

These states are diagnostic in this replay layer. The replay layer does not
silently invent a spread, remove a crossed level, or otherwise repair source
state.

An empty-side state cannot produce a complete checkpoint because the checkpoint
contract requires at least one positive bid and one positive ask level.

### Reconstruction order

Reconstruction is performed separately for each venue and instrument.

Cross-market aggregation is allowed only after every contributing market has
independently established a valid reconstructed state.

Exchange and instrument identity remain in internal provenance even when the UI
does not expose exchange filters.

## Bucket sampling

The base derived resolution is one second.

The authoritative sampling clock for the current CryptoHFTData Binance Futures
reader is:

```text
received_time_ns
```

Exchange `event_time` and `transaction_time` remain available for diagnostics
and provenance, but they do not reorder or own one-second sampling.

Each completed UTC source hour contains exactly:

```text
3,600 one-second observations
```

Bucket endpoints run from:

```text
HH:00:01
through
the next exact UTC hour
```

The sampled state for a bucket is:

> The last reconstructed replay state observed at or before the bucket end.

An event whose `received_time_ns` is exactly equal to the bucket endpoint is
included in that bucket.

If multiple normalized events share the same `received_time_ns`, all such
events are applied in archive order before the sample at that timestamp is
emitted.

This is an end-of-bucket state observation. It is not a time-weighted average.

A quiet second with no update does not automatically invalidate the existing
book. The state remains usable while initialization, sequence continuity, and
book structure remain valid.

A verified checkpoint or carried state from the immediately preceding UTC hour
is eligible from the first bucket of the next hour, even when no new event has
yet arrived in that hour.

One-second analytical book quality is:

```text
NORMAL:
    VALID

LOCKED:
    INVALID

CROSSED:
    INVALID

EMPTY_BID:
    INVALID

EMPTY_ASK:
    INVALID

EMPTY_BOTH:
    INVALID

uninitialized replay:
    INVALID

continuity-invalidated replay:
    INVALID
```

A replay state may remain sequence-valid while its sampled analytical state is
invalid. In particular, locked, crossed, and empty-side states are not silently
repaired and cannot produce valid liquidity observations.

An invalid sample remains represented explicitly. It must not be replaced with
the most recent valid numerical sample merely to make a continuous series.

### Depth-band liquidity

For each market, depth is calculated relative to that market's current best
prices.

Bid interval:

```text
[
    best_bid * (1 - upper_depth_fraction),
    best_bid * (1 - lower_depth_fraction)
]
```

Ask interval:

```text
[
    best_ask * (1 + lower_depth_fraction),
    best_ask * (1 + upper_depth_fraction)
]
```

For an approved linear USD-equivalent market:

```text
level_notional = price x quantity
```

Then:

```text
Bid Liquidity = sum(valid bid-level notional inside bid interval)
Ask Liquidity = sum(valid ask-level notional inside ask interval)
Total Liquidity = Bid Liquidity + Ask Liquidity
```

Bid-Ask Imbalance is:

```text
(Bid Liquidity - Ask Liquidity)
/
(Bid Liquidity + Ask Liquidity)
```

If the denominator is zero or invalid, imbalance is invalid. It is not silently
replaced with zero.

Bid Liquidity, Ask Liquidity, and Total Liquidity are accumulated as exact
finite `Decimal` values at the reconstruction/liquidity boundary.

Because imbalance division may produce a repeating decimal, its in-memory
`Decimal` representation uses an explicit deterministic precision. The initial
implementation uses:

```text
34 significant decimal digits
ROUND_HALF_EVEN
```

The selected precision must remain recorded with the result or encoding
contract. Changing it is a representation-policy change and must not happen
silently.

Depth interval boundaries are inclusive. A level whose exact price equals a
lower or upper boundary contributes to liquidity.

Depth queries use exact live side-price indexes to locate the requested range.
They must not copy or scan the complete order-book dictionaries for every
one-second observation.

One-second liquidity emission runs at safe synchronous replay-state query
boundaries. Replay state is borrowed only while emitting a bucket and must not
be retained, mutated, transferred to another thread, or used asynchronously.

For an event with authoritative sampling time `received_time_ns`, every bucket
ending strictly before that event is emitted from the preceding reconstructed
state. The event is then applied. Therefore an event exactly at a bucket end is
included in that bucket.

When several events share one `received_time_ns`, no bucket at that timestamp is
emitted until all same-timestamp events have been applied in archive order.

Liquidity is calculated only for analytically `VALID` one-second book states.
Invalid observations retain explicit quality and reason fields, while Bid
Liquidity, Ask Liquidity, Total Liquidity, and Bid-Ask Imbalance remain null.
Invalid observations are never populated from a formerly valid state.

A valid reconstructed book whose selected depth band contains no contributing
levels has:

```text
Bid Liquidity = 0
Ask Liquidity = 0
Total Liquidity = 0
Bid-Ask Imbalance = invalid/null
```

The book observation itself remains valid. A zero imbalance denominator does
not retroactively make the reconstructed book invalid.

The initial in-memory hourly liquidity block contains exactly 3,600 observations
and records:

```text
Bid Liquidity
Ask Liquidity
derived Total Liquidity
derived Bid-Ask Imbalance
quality
invalid reason
single-market source count
depth-level counts
sampling provenance
```

Long-term compact storage still treats Bid Liquidity and Ask Liquidity as the
authoritative metric channels. Total Liquidity and Bid-Ask Imbalance remain
derived values and need not be stored as separate PostgreSQL blocks.

Coin-margined and inverse contracts remain unsupported until authoritative
venue-specific contract metadata and notional conversion are implemented.

### Compact hourly liquidity block encoding

Each completed single-market UTC source hour contains exactly 3,600 analytical
slots.

The compact PostgreSQL representation stores four authoritative binary
channels:

```text
bid_liquidity_block
ask_liquidity_block
validity_block
source_count_block
```

Each channel uses:

```text
versioned l2shock binary envelope
+
Zstandard-compressed Arrow IPC stream
```

The envelope records:

```text
format version
channel identity
observation count
Arrow payload length
Arrow payload SHA-256
```

The four channels also receive one deterministic combined content SHA-256.

Bid and Ask Liquidity remain exact non-negative `Decimal` values. They are
encoded as canonical non-exponent decimal strings inside Arrow rather than
being forced into an unproven fixed decimal scale.

Canonical decimal encoding removes insignificant trailing zeros:

```text
0.0300 -> 0.03
123.4500 -> 123.45
```

This changes representation only, not mathematical value.

The validity channel uses explicit fixed format-version-1 integer codes.

Quality codes are:

```text
1 = VALID
2 = DEGRADED
3 = INVALID
```

Invalid-reason codes are:

```text
0 = no invalid reason
1 = uninitialized
2 = replay_invalidated
3 = locked
4 = crossed
5 = empty_bid
6 = empty_ask
7 = empty_both
```

These numeric values are persistence-format identities. Existing values must
never be renumbered because of Python enum declaration order. An incompatible
mapping requires a new block format version.

The source-count channel uses unsigned 16-bit integers. A valid observation
requires at least one contributing source. A non-valid observation has source
count zero.

Total Liquidity and Bid-Ask Imbalance are not stored as independent block
channels. They are derived from the decoded Bid and Ask channels.

Decoding validates:

```text
binary magic
format version
channel identity
flags
observation count
payload length
payload SHA-256
Arrow schema
Arrow metadata
exact row count
quality/reason consistency
liquidity nullability
source-count consistency
combined four-channel SHA-256
```

Encoded block hashes detect corruption but do not authenticate the producer.

The database row owns the exact:

```text
base
UTC hour
data-preset hash
schema version
provenance
quality summary
```

The channel bytes do not duplicate those row-level identities.

### Stored metric policy

Long-term storage should contain the minimal authoritative derived channels:

- Bid Liquidity;
- Ask Liquidity;
- validity/quality state;
- contributing-source count;
- required provenance.

The following are derived on demand and should not normally be stored
separately:

- Total Liquidity;
- Bid-Ask Imbalance;
- 5s, 10s, 15s, 30s;
- minute, hour, day, and week timeframes.


### Fixed-duration timeframe aggregation

The stored one-second series is the authoritative analytical base.
The detector-free Analysis workflow loads verified observations and reduces
them into viewing bars. It performs no detection, hypothesis generation,
channel evidence scoring, ranking, or candidate annotation.

Viewing timeframe changes do not rewrite persisted one-second data or alter
the semantic data-preset identity.

#### Detector-free Analysis viewing bars

The active viewing pipeline is implemented in:

```text
l2shock/analysis/l2_view_stream.py
```

Supported viewing timeframes are:

```text
Auto
1s
5s
15s
1m
5m
15m
1h
4h
1d
```

Viewing bars are aligned to UTC multiples of the selected fixed duration.
The first and last bars may contain only the portion of their interval
owned by the requested range.

Auto selects the finest supported viewing timeframe whose bar count fits
Maximum viewing bars. The supported bar budget is 1-5000, with a default
of 1200.

An explicit timeframe that does not fit the budget is rejected. It is
never silently coarsened. The bar budget changes viewing granularity;
it never truncates the requested source interval.

A viewing bar remains renderable when at least one numerical L2 second
contributes. Missing and invalid seconds do not contribute numerical values,
but remain included in source ownership and quality counts.

Partial-market seconds contribute the exact same-second sum of their valid
markets and remain explicitly counted as partial coverage. Unavailable
observations are never zero-filled or forward-filled.

Poor coverage, whether sparse or across the whole request, produces warnings
rather than a rendering veto. A bar with no numerical L2 observations remains
null because no actual values exist to plot.

Bid and Ask candles are reduced from their usable one-second values.
Total and Delta candles are reduced from same-second Total and Delta:

```text
Total(t) = Bid(t) + Ask(t)
Delta(t) = Bid(t) - Ask(t)
```

Their extrema must not be constructed by adding or subtracting independent
Bid and Ask OHLC extrema that occurred at different seconds.

Percentage candles are reduced from one-second percentage values.
Undefined denominators remain null; they are not converted to zero.
A bar without the required defined percentage values must not acquire
a fabricated percentage candle through cached coarsening.

Change metrics compare adjacent usable viewing-bar closes at the selected
viewing timeframe. The first bar and a bar following an unusable bar have
no change value.

Only valid real-trade price seconds contribute to Price OHLC. Missing
price seconds remain coverage facts, and a price bar with no contributing
real trades is null. Price unavailability never makes otherwise usable
L2 bars unavailable.

Compatible coarser views can be built from the retained projection without
a database reread. A finer uncached view requires a cancellable background
reload. Cached coarsening must preserve the same source interval, available-observation
L2 policy, percentage undefined-value policy, and quality counts.

Panel and warning changes operate on the currently displayed projection.
They do not validate unrelated loading controls or admit another load.

#### Duration and streaming-budget controls

Max scan duration is an editable limit measured in owned one-second slots:

```text
default: 86,400 seconds
minimum configurable limit: 3,600 seconds
maximum configurable limit: 63,072,000 seconds
```

The minimum applies to the configurable limit, not to the length of every
Analysis request. A short request, including one owned second, remains
valid when it fits that limit.

The user's closed endpoints each own their containing UTC second:

```text
closed inputs:
    [12:00:07.5, 12:00:09.1]

effective half-open slot range:
    [12:00:07, 12:00:10)
```

Both endpoint seconds count toward the duration limit. The upper ceiling
is 730 days of owned seconds; it is not calendar-year arithmetic.

Streaming memory budget is separately editable:

```text
default: 512 MiB
minimum: 64 MiB
maximum: 16,384 MiB
```

The budget selects bounded hour-chunk sizes. It is a chunk-size heuristic,
not a guaranteed total-process RAM ceiling. Decoding, Python objects,
repository results, the retained projection, chart options, and browser
serialization also require memory.

A long requested duration does not imply retaining every one-second
observation for the complete range. Verified hour chunks are decoded,
composed, and folded into bounded viewing bars.

Long ranges can still require substantial reading, decoding, and
verification time. The UI warns for ranges above 183 days and for a
streaming budget above 6 GiB; those warnings do not automatically reject
an otherwise valid request.

The daily viewing-bar budget remains a separate constraint. Increasing
Max scan duration does not make a range fit a smaller Maximum viewing bars
setting.

#### Retained aggregation foundation

These modules remain tested foundation:

```text
l2shock/analysis/timeframes.py
l2shock/analysis/aggregation.py
```

`aggregation.py` also owns the `L2Second` type used by verified decoding
and market composition.

The foundation's snapping and endpoint-state aggregation functions are
not substitutes for the active streaming viewing-bar policy.

The foundation supports these fixed-duration timeframes:

```text
1s
5s
10s
15s
30s
1m
3m
5m
10m
15m
30m
45m
1h
2h
4h
8h
12h
1d
3d
1w
```

Calendar month timeframes (`1M`, `3M`, `6M`) remain deferred until a separate
calendar-aware ownership contract is implemented. A month must never be
silently represented as a fixed number of seconds.

For a closed continuous-time range `[m, n]` and timeframe `T`, the
foundation's bar-aligned loading range is:

```text
snapped_start = floor(m, T)
snapped_end   = floor(n, T) + T

effective range = [snapped_start, snapped_end)
```

Its automatic timeframe selection picks the finest supported timeframe
whose snapped bar count fits the caller's budget.

In the foundation, a larger L2 state bar owns its final one-second
endpoint state:

```text
all seconds valid and endpoint valid:
    VALID

endpoint valid but an earlier second is invalid or missing:
    DEGRADED

endpoint invalid or missing:
    INVALID
```

That retained endpoint policy differs intentionally from the active
Analysis viewing policy, which reduces available numerical seconds into
OHLC even when the final owned second is unavailable. Coverage limitations
remain explicit and do not veto rendering.

The foundation's real-trade price aggregation uses:

```text
Open:
    first valid real-trade Open

High:
    maximum valid real-trade High

Low:
    minimum valid real-trade Low

Close:
    final valid real-trade Close

trade_count:
    sum of valid one-second trade counts
```

Its price aggregate quality is:

```text
all seconds contain valid real-trade candles:
    VALID

some but not all seconds contain valid real-trade candles:
    DEGRADED

no second contains a valid real-trade candle:
    INVALID
```

Invalid and missing lower-level price observations remain explicit
coverage facts. They are not forward-filled. The foundation records a
hard discontinuity even when real trades elsewhere in that interval
permit a degraded OHLC bar.

Fixed-duration bucket alignment remains UTC internally. Display timezone
conversion does not change bucket identity.

### Verified detector-free Analysis loading

`l2shock/analysis/l2_view_stream.py` loads compact component L2 rows through
the verified `AnalyticalRepository` read boundary with `verify_codec=True`.

Verified loading checks the stored preset content against its hash and
resolves aggregate presets into their deterministic single-market
component preset identities.

The normal repository verification boundary checks:

```text
row identity
combined content SHA-256
per-channel envelope and payload hashes
Arrow schema and metadata
exact 3,600-slot ownership
quality-summary consistency
typed source provenance
```

A missing persisted component hour contributes explicit unavailable
coverage. Missing observations are never zero-filled or forward-filled.

Single-market presets use their verified component directly.
Multi-market presets compose observations at the same exact UTC second:

```text
every expected market contributes valid Bid and Ask:
    exact component sums; complete-market usable L2 second

some, but not all, expected markets contribute:
    partial-market second; exact available-market sum rendered

no expected market contributes:
    numerically unavailable L2 second
```

Partial-market coverage does not reject the complete Analysis request.
It contributes numerical sums to viewing bars and remains included in
diagnostics and warning regions. Usable means numerically available,
not necessarily complete-market coverage.

The loader does not merge raw events, snapshots, replay frontiers, or
checkpoints across markets. Aggregate presets own no separately persisted
aggregate L2 rows.

Optional Binance price is loaded through its independent verified price
repository. Missing price does not block L2. A price-loading failure is
reported separately rather than interpreted as proof that every affected
second contained no trades.

Repository sessions are scoped to bounded reads rather than retained for
the whole Analysis operation. Cancellation is checked during loading,
and a stopped or failed load does not publish a partial replacement
projection.


### Price source identity

Price charts use real Binance USD-M USDT perpetual trades as optional context
(never a Shock-Start detection input):

```text
BTCUSDT
ETHUSDT
```

Order-book midpoint is not presented as real traded OHLC.

The authoritative price-candle assignment clock is:

```text
trade_time_ms
```

`received_time_ns` and `event_time_ms` remain available for source diagnostics
and provenance, but do not own candle assignment.

One-second trade candles use half-open intervals:

```text
[bucket_start, bucket_end)
```

A trade exactly at a second boundary belongs to the new second beginning at
that boundary.

Open and Close are selected by real trade time. When several distinct trades
share the same `trade_time_ms`, their deterministic streamed source order breaks
the tie:

```text
first observed at that trade time -> Open candidate
last observed at that trade time  -> Close candidate
```

A second containing no real trade is explicitly invalid. It is not
forward-filled and is not replaced with an order-book midpoint candle.

Trade records from adjacent source archives may later be routed into their
actual trade-time-owned UTC hour. Trades outside a requested target hour must be
counted explicitly rather than silently discarded.

No spot or alternative-exchange price source may silently replace the Binance
perpetual source.

### Compact hourly trade-price block encoding

Each completed Binance Futures trade-time UTC hour contains exactly:

```text
3,600 one-second price slots
```

The compact PostgreSQL representation stores three binary channels:

```text
ohlc_block
validity_block
trade_count_block
```

Each channel uses:

```text
versioned l2shock binary envelope
+
Zstandard-compressed Arrow IPC stream
```

The OHLC channel stores exact canonical non-exponent decimal strings:

```text
Open
High
Low
Close
```

Insignificant decimal scale does not create a different encoded identity:

```text
100.2500 -> 100.25
```

The original mathematical Decimal value is preserved. Floating-point OHLC is
not introduced at the price-construction or persistence boundary.

Trade-price quality codes are explicit format-version-1 identities:

```text
1 = VALID
2 = DEGRADED
3 = INVALID
```

`DEGRADED` is reserved but is not currently emitted by one-second trade OHLC
construction.

Trade-price invalid-reason codes are:

```text
0 = no invalid reason
1 = no_trades
```

These numeric values must never be derived from Python enum declaration order
or renumbered inside format version 1.

The validity contract is:

```text
VALID:
    all OHLC values are positive exact decimals
    trade_count > 0
    invalid reason = none

INVALID:
    OHLC values are null
    trade_count = 0
    invalid reason = no_trades
```

No-trade slots remain explicit and are not forward-filled.

The trade-count channel uses unsigned 32-bit integers. The representation
therefore rejects a per-second trade count outside the format-version-1 range.

Each channel envelope records:

```text
binary magic
format version
channel identity
flags
observation count
Arrow payload length
Arrow payload SHA-256
```

The complete three-channel artifact also receives one deterministic combined
content SHA-256.

Decoding validates:

```text
binary magic
format version
channel destination
flags
payload length
payload SHA-256
Arrow schema
Arrow metadata
exact 3,600-row ownership
canonical decimal representation
OHLC geometry
quality/reason consistency
trade-count consistency
combined three-channel SHA-256
```

The encoded channel bytes do not duplicate row-level PostgreSQL identity such
as:

```text
base
UTC hour
source venue
source symbol
quality summary
source provenance
```

Those identities belong to the database row.

Hashes detect corruption and deterministic content identity. They do not
authenticate the producer or prove that source archives were trustworthy.


### Trade-price PostgreSQL write contract

Compact trade-price persistence is idempotent by identity.

A price hour is identified by:

```text
base
trade-time-owned UTC hour
```

The price source is fixed in version 1:

```text
BTC -> Binance Futures BTCUSDT
ETH -> Binance Futures ETHUSDT
```

An identical repeated write is accepted as an idempotent no-op.

A repeated write with different:

```text
OHLC channel bytes
validity channel bytes
trade-count channel bytes
content SHA-256
codec or format version
quality summary
source provenance
```

is rejected as a conflict. Existing price content must never be silently
replaced.

Every write decodes and verifies the complete encoded price artifact before
issuing the PostgreSQL insert. Every normal read reconstructs and verifies:

```text
combined content SHA-256
per-channel envelopes
per-channel payload SHA-256
channel ownership
Arrow schemas and metadata
exact row count
OHLC decimal canonicality
OHLC geometry
quality/reason consistency
trade-count consistency
quality-summary consistency
```

Price-hour provenance contains versioned identities for every CryptoHFTData
trade archive streamed while constructing the target hour.

Each source identity contains:

```text
provider
venue
instrument
data kind = trades
UTC source-file hour
source archive SHA-256
```

Because the authoritative candle clock is `trade_time_ms`, the processor may
stream the immediately previous, current, and immediately following source
archives when routing records near source-file boundaries.

The current source hour must always be included in provenance. A source more
than one hour away from the target hour is rejected by the version-1
provenance contract.

Local filesystem paths are operational details and are not durable price
identity.

Range reads use half-open UTC bounds:

```text
[start_utc, end_utc)
```

The price repository does not commit or roll back transactions. Transaction
ownership belongs to the caller.

A successful price-hour write does not by itself change:

```text
source_hours.status
source_hours.quality_state
source_hours.processed_at
```

Those transitions belong to the later processing coordinator.


### Production trade-price processing

The production price coordinator builds one trade-time-owned UTC hour from an
exact, explicitly selected source set.

Version 1 defaults to:

```text
current source archive only
```

This default is deterministic. A neighboring archive becoming available later
must not silently change an already-persisted price-hour provenance identity.

An operation may explicitly request:

```text
immediately previous source archive, when locally replayable
current source archive, required
immediately following source archive, when locally replayable
```

The exact selected sources are recorded in typed price provenance.

All selected files are rechecked immediately before streaming:

```text
regular-file ownership
durable file size
complete file SHA-256
```

The processing sequence is:

```text
source lookup and advisory lock
-> source SHA-256 verification
-> streamed trade parsing
-> trade_time_ms routing
-> one-second real-trade OHLC
-> compact price encoding
-> typed provenance
-> idempotent PostgreSQL persistence
-> target trade-source terminal metadata
```

The operational target-source quality state is:

```text
3,600 valid one-second candles:
    VALID

1-3,599 valid one-second candles:
    DEGRADED

0 valid one-second candles:
    INVALID
```

This source-hour summary does not change the fixed per-second price quality
codes.

Cancellation is honored until immediately before price persistence. Once the
short persistence/finalization transaction begins, it is completed rather than
misreported as safely cancelled.

Only the current target trade source is changed to `processed`. Adjacent source
archives used for routing retain their existing operational state.


### Data-quality states

Derived observations use:

```text
VALID
DEGRADED
INVALID
```

Invalid observations are never converted into valid observations by
forward-fill.

A valid reconstructed book may retain its last state through a quiet
bucket, but only while initialization, sequence continuity, and book
structure remain proven.

The active Analysis viewing policy uses available verified numerical L2
observations. A viewing bar remains renderable when at least one numerical
second contributes; partial-market sums are permitted and explicitly counted.

Data-quality limitations do not veto Analysis or suppress available candles.
If the entire range has no numerical L2 observations, Analysis still completes,
but the L2 channels remain null rather than acquiring fabricated values.

Undefined percentage denominators remain null. A contributing zero-total
second makes that bar's percentage candle undefined without suppressing its
Bid, Ask, Total, or Delta candle.

Usable-second counts include partial-market numerical observations.
Partial-market counts are a subset of usable-second counts, not an additional
category to add to usable plus unusable.

The user cross-checks price and L2 with trdr.io before trading. Coverage
warnings remain important because changing contributors can create apparent
metric movements.

Persistent red data-outage warning regions are measured over the
effective requested one-second range:

```text
L2:
    invalid, missing, or partial-market coverage for strictly more than
    analysis.l2_long_invalid_warning_seconds consecutive seconds
    default: 60 seconds
    drawn on all five panels

Price:
    absence of a valid Binance real-trade candle for strictly more than
    analysis.price_long_invalid_warning_minutes consecutive minutes
    default: 3 minutes
    drawn on the Price panel only
```

Outage tracking continues across hour and chunk boundaries. The active
streaming loader does not use the retired bounded viewport's
threshold-padded price-warning query.

A run exactly equal to the threshold is not flagged. Missing, invalid,
and partial-market seconds join the same uninterrupted L2 quality-warning
run. Only complete-market usable coverage ends that L2 run.

Partial-market numerical values can therefore remain visible beneath a
quality-warning overlay. Price warning runs remain independent.

The "Show data-quality warnings" switch is on by default. Changing it
rebuilds presentation from the displayed projection without rereading
the database.

Warning regions are presentation metadata. Their visibility does not
change liquidity values, bar eligibility, source ownership, or stored
data.

Each channel retains at most 5,000 warning regions. If the cap is reached,
the UI reports truncation. Quality counts remain available; the chart
must not imply that every outage was highlighted.

Missing verified price observations can produce price-outage regions.
A failed or corrupt price load is different: the loader reports that
status and returns no price-outage regions for that load. It must not
fabricate a no-trades conclusion from a transport or verification failure.

L2 can still be rendered when optional price loading fails. The UI
reports that price outage highlighting is unavailable for that load.

### Application-owned Analysis runtime

Verified loading, compact-channel decoding, exact-second market composition,
and viewing-bar reduction run in a synchronous worker thread owned by
`l2shock/ui/l2_view_runtime.py`.

The operation name is `manual_analysis_load`. The NiceGUI event loop remains
available for progress updates and cooperative Stop requests.

Analysis loading shares the process-wide operation admission boundary with
Manual Fetch, Manual Processing, and Remote HF Import. Runtime admission
rejects an existing operation name, an owned operation lock, or the shutdown
barrier before admitting another load.

Database sessions are created and closed inside the worker thread which uses
them. ORM objects never cross the worker/event-loop boundary.

Stop is cooperative. Cancelling the owning asyncio task does not terminate
the synchronous worker. The runtime retains operation ownership until the
worker exits, including under repeated cancellation.

A Stop or shutdown request observed before execution prevents the loader from
being launched. Cancellation before the task's first execution has an explicit
finalization owner so it cannot strand the admission reservation.

A stopped, cancelled, or failed load does not publish a partial projection.
The last successful runtime projection remains available. Runtime completion
and browser chart publication are separate boundaries: successful loading
does not by itself prove that a replacement chart was acknowledged.

Application shutdown starts from the confirmed global header Shutdown button.
`l2shock/ui/shutdown.py` is authoritative for detailed ordering and timeout
behavior.

Shutdown raises admission barriers, stops the retained operation runtimes,
and resolves operation-lock ownership, tracked tasks, and registered
database readers before disposing the SQLAlchemy engine.

If background-work ownership cannot be resolved safely, shutdown is not
published as complete and the engine is not disposed. Admission remains
blocked; the header reports the failure and permits a shutdown retry.

Analysis projections and chart options are process-local objects. They are
not PostgreSQL rows or database backups.


### Timezone contract

All persisted timestamps use timezone-aware UTC.

User inputs and all user-visible times use the configured IANA timezone.

Default:

```text
Asia/Tehran
```

Ambiguous or nonexistent DST local times must be rejected rather than
silently resolved.

Charts keep UTC coordinates internally: axis data, warning regions, and zoom
capture and restoration; chart coordinates stay UTC. Only presentation is localized. Time-axis labels,
the crosshair label, and the tooltip header use the configured timezone, and
the tooltip header names it.

Analysis, Fetch, Processing, and Remote Import range inputs, Automatic Fetch
status, maintenance preview expiry, Settings timestamps, and diagnostics
report time are shown in the configured timezone.

UTC remains authoritative for storage, source-hour identity, analytical
identity, and exported files. Local-time display never changes bucket
ownership.

Chart localization is owned by `l2shock/ui/display_timezone.py`. It must not
depend on retired detector annotation modules.

### Preset identity

Data-affecting preset settings receive a deterministic SHA-256 hash over
canonical UTF-8 JSON.

The canonical object includes:

```text
preset schema identity and version
liquidity algorithm version
base asset
sampling clock and interval
bucket sampling policy
depth lower and upper fractions
inclusive depth-boundary policy
independent bid/ask anchor policy
notional convention
cross-market aggregation policy
book-quality policy
complete ordered eligible-market snapshot
```

Exact depth fractions are canonical non-exponent decimal strings. Insignificant
decimal scale does not create a different identity:

```text
0.0100 == 0.01
```

Eligible-market input order also does not create a different identity. Markets
are sorted by complete semantic identity before canonical JSON encoding.

Each eligible market identifies:

```text
provider
venue
instrument
base asset
quote asset
market type
settlement type
```

For V1, supported notional ownership is restricted to approved spot or linear
USD-equivalent markets. Coin-margined/inverse conversion remains unsupported.

The initial materialized preset contains one Binance Futures perpetual market
for the selected base:

```text
BTC -> BTCUSDT
ETH -> ETHUSDT
```

This is the initial validated market universe, not a permanent architectural
restriction. Future markets may be added only through a new semantic preset
identity after their reconstruction and notional rules are proven.

Display and analysis-presentation settings are excluded from the data-preset
hash, including:

```text
UI timezone
chart colors
visible panels
chart timeframe
activity timeframe
Shock-Start scale, evidence, and inspection-order settings
Top-N
annotation visibility switches
optional price context
temporary UI state
```

Enabling or disabling materialization is operational state and does not mutate
the semantic preset hash.

Editing a semantic preset creates a new hash. Existing derived data remains
owned by the old hash.

When a processed raw order-book archive is still locally available, normal
manual Processing may select it again only when the exact requested component
single-market preset row is absent.

Materialization-aware processing therefore supports creation of a new depth
configuration without rewriting the existing analytical row:

```text
processed raw source
+
new component preset hash absent
+
intact local raw archive
->
eligible materialization target
```

An already materialized identical component row is skipped.

A processed Binance trade source is selected again only when its fixed
price-hour row is absent.

Processed raw sources whose local archive was pruned are not selected. They
require the separate raw-rehydration workflow before a new semantic preset can
be materialized.

Source operational quality metadata retains every known L2 analytical content
hash for the raw source hour. A later materialization must not make an earlier
preset-owned row appear orphaned.

If raw source files were deleted, a new semantic preset cannot be reconstructed
historically unless the required source files can be downloaded again.

#### Preset CRUD and immutable semantic editing

The Settings tab provides operational CRUD controls for persisted data presets.

Preset semantic content is immutable after insertion.

Creating or editing data-affecting values such as:

```text
base
eligible markets
depth lower fraction
depth upper fraction
sampling policy
quality policy
notional convention
algorithm version
```

creates a new canonical preset identity and therefore a new `preset_hash`.

Editing must never overwrite an existing preset’s canonical JSON or reinterpret
historical L2 rows as belonging to another semantic configuration.

Enable/disable state is operational:

```text
enabled:
    eligible for current selection/materialization workflows

disabled:
    retained but not preferred for new workflows
```

Changing enable state does not change the preset hash.

Deletion is fail-closed. A preset may be deleted only when:

```text
enabled = false
and
no l2_hourly_series row references the preset hash
```

Preset deletion never cascades into analytical-row deletion.

The Settings editor supports the exact approved profiles:

```text
Binance Futures single market
Bybit single market
OKX Futures single market
Binance + OKX aggregate analysis identity
Binance + Bybit + OKX aggregate analysis identity
```

Unsupported or unrecognized market compositions remain read-only and must not
be reinterpreted through this editor.


#### Approved Preset CRUD market profiles

The Settings editor supports these exact market compositions:

```text
Binance Futures:
    BTCUSDT or ETHUSDT

Bybit:
    BTCUSDT or ETHUSDT

OKX Futures:
    BTC-USDT-SWAP or ETH-USDT-SWAP

Binance + OKX Futures aggregate:
    both independently reconstructed component markets

Binance + Bybit + OKX Futures aggregate:
    all three independently reconstructed component markets
```

Changing market composition is a semantic preset edit. It creates a new
immutable preset hash.

The aggregate preset does not own a separately persisted aggregate L2 row.
Instead:

```text
aggregate preset
    -> deterministic component single-market preset hashes
    -> verified component l2_hourly_series rows
    -> per-second sum of valid component markets at analysis load time
```

If every expected component market contributes during one second, aggregate
coverage is `VALID`.

If at least one but fewer than all expected markets contribute, the pure
aggregation layer represents the second as `DEGRADED` and retains the exact
partial sum for diagnostics. The verified Shock-Start loader rejects the complete
scan when such a second lies inside the selected range.

The missing market is never interpreted as zero liquidity.

If no expected market contributes, aggregate coverage is `INVALID`.

The original single-market Binance and OKX presets remain unchanged and retain
their existing hashes.


### Analytical PostgreSQL write contract

Compact analytical persistence is idempotent by identity.

A data preset is inserted by:

```text
preset_hash
```

The hash owns immutable semantic content:

```text
schema version
algorithm version
base
canonical preset JSON
```

If an existing hash owns different semantic content, persistence fails with an
identity conflict. It must never overwrite or reinterpret the existing preset.

Preset enable/disable state is operational state. Changing it does not change
the semantic preset hash and does not delete historical derived data.

An L2 hourly series is identified by:

```text
base
UTC hour
preset hash
```

An identical repeated write is accepted as an idempotent no-op.

A repeated write with different:

```text
channel bytes
content SHA-256
codec or format version
quality summary
source provenance
```

is rejected as a conflict. Existing analytical content must never be silently
replaced.

Every write verifies the complete encoded block before issuing the PostgreSQL
insert. Every normal read reconstructs the encoded artifact and verifies:

```text
combined content SHA-256
per-channel envelopes
per-channel payload hashes
channel ownership
Arrow schemas and metadata
row counts
quality/reason consistency
source-count consistency
```

The analytical repository does not commit or roll back transactions. Transaction
ownership belongs to the caller.

L2 hourly provenance uses a versioned JSON object containing:

```text
provenance schema identity and version
replay schema version
liquidity schema version
current and relevant predecessor source-hour identities
source archive SHA-256 values
optional serialized checkpoint SHA-256
```

A source-hour identity contains:

```text
provider
venue
instrument
data kind
UTC source hour
source archive SHA-256
```

Local filesystem paths are operational details and are not durable analytical
identity.

At least one referenced order-book source must belong to the persisted
analytical hour. Provenance must not reference a future source hour.

Range reads use half-open UTC bounds:

```text
[start_utc, end_utc)
```

This repository persists no raw L2 rows and does not promote acquisition or
reconstruction status. Processing orchestration owns those later transitions.

### Analysis identity and exports

The verified streaming loader owns the deterministic `input_id` of its
loaded Analysis projection.

The identity is derived from the effective UTC source range, base,
immutable preset and expected component identities, and the verified
hourly content consumed by the loader. Missing component availability
remains explicit.

Use the current `l2shock/analysis/l2_view_stream.py` implementation as the
authority for the complete canonical identity payload. Do not reconstruct
that payload from an older detector proposal.

There is no detector `scan_id`, evidence identity, inspection-order
version, or `review_id` in the active Analysis workflow.

Panel selections, chart colors, timezone labels, browser zoom, and warning
visibility are presentation state. Cached coarsening preserves the loaded
projection's input identity.

Each acknowledged browser publication separately owns a render token.
An input identity identifies loaded data; a render token identifies one
specific browser publication. They are not interchangeable.

JSON and CSV export the full displayed-bar dataset, including the current
viewing timeframe and selected panel metadata. They do not export only
the current browser zoom window.

PNG and SVG export the acknowledged chart presentation.

Analysis exports are research artifacts, not PostgreSQL backups.


### Explicit non-goals

Version 1 does not include:

- raw L2 PostgreSQL storage;
- L3 individual-order reconstruction;
- smart-money identity classification;
- automatic strategy generation;
- live trade execution;
- machine learning;
- arbitrary FX conversion;
- unsupported inverse-contract notional guessing;
- independent local-extrema detector;
- exchange-selection UI;
- interactive full-depth heatmap.

---

### Detector-free Analysis semantic contract

Analysis loads verified historical data, constructs viewing bars, and
renders them. It does not generate hypotheses, detect start areas, score
evidence, rank results, or produce candidate A/B/C annotations.

The active analytical contracts are:

```text
source clock:
    verified one-second UTC ownership

expected markets:
    exact immutable component identities resolved from the preset

usable multi-market second:
    at least one expected component contributes valid Bid and Ask

partial-market second:
    exact available-market sum rendered with explicit quality warnings

viewing-bar L2 policy:
    available numerical seconds contribute; no quality-based rendering veto

Total and Delta:
    constructed from Bid and Ask of the same second

percentage candles:
    reduced from defined one-second ratios, never independent OHLC extrema

undefined denominator:
    null, never zero

change metrics:
    adjacent usable viewing-bar closes at the selected timeframe

optional price:
    independent verified real-trade context; never required for usable L2

persistent storage:
    unchanged authoritative component-market and trade-price blocks
```

The selectable registry contains exactly eleven metrics. The current
registry in `l2shock/analysis/l2_view_metrics.py` is authoritative.

Panel A defaults to Signed Imbalance %. Panel B defaults to Order-Book
Delta. Bid/Ask Shares % is a two-line percentage composition metric,
not a raw Bid/Ask ratio.

No price-matched liquidity-flow mode or snapshot-flow substitute is
offered.

Changes to these mathematics, gap policies, endpoint ownership, market
composition, or undefined-value rules require explicit semantic review.
Presentation-only changes do not rewrite persisted analytical rows or
preset identities.

### Analysis chart publication and interaction ownership

Every complete publication owns the stored widget option and a hidden
browser render-token series.

Python-side validation must complete before withdrawing a previously
acknowledged controller commit for invalid caller input. Strict JSON
serialization must complete before mutating the widget's stored options
or render ownership.

A successful Python publication request is not browser acknowledgement.
The controller commits a replacement only after browser identity and
layout verification and a final ownership check.

The compact live-state probe returns bounded facts, not the complete
browser `getOption()` payload.

Complete replacement uses the existing explicit publication path.
Do not introduce an additional competing complete-option writer or
wrap `instance.setOption` to force resets on every interaction.

Panel and warning changes rebuild the displayed projection without a
database reread. Timeframe and bar-budget changes may use compatible
cached coarsening or admit a cancellable reload.

Normal wheel interaction controls synchronized X zoom. Shift+wheel
controls independent Y zoom in the hovered panel.

Changing a metric resets only that panel's Y zoom. Changing the effective
viewing timeframe resets all Y zooms. Timeframe changes preserve the
captured UTC left edge and visible duration where the new bounds permit.

Crosshair callbacks and pending animation frames belong to one chart and
one installation generation. Obsolete callbacks must not remove graphics
or cancel frames belonging to a newer installation.

PNG/SVG export requires acknowledged chart ownership. JSON/CSV export
requires the displayed projection and its acknowledged owner to agree.

A stopped or failed data load does not publish a partial projection.
A browser publication failure is a separate boundary: after replacement
begins, automatic restoration of the prior browser option is not
guaranteed. Do not describe failed acknowledgement as proof that the old
browser chart was restored.

## Development workflow

Implementation order:

1. project and database foundation;
2. downloader and atomic raw cache;
3. streaming Parquet reader;
4. cross-hour initialization proof;
5. reconstruction;
6. replay/sequence validation;
7. depth calculation;
8. market eligibility and preset identity;
9. compact hourly storage;
10. trade-price OHLC;
11. timeframe aggregation;
12. verified bounded-chunk Analysis loading and viewing-bar construction;
13. orchestration and caching;
14. Fetch and Settings UI;
15. Analysis UI;
16. synchronized charts;
17. exports and hardening.

Reconstruction and liquidity arithmetic must be proven before chart work begins.

<!-- l2shock:b2-transport-status -->
Staged Backblaze B2 migration
Backblaze B2 is being added as storage for verified processed artifacts.
This is separate from any future CryptoHFTData S3 source-archive backfill
transport.

The B2 object transport supports bounded SDK retries, Retry-After handling,
pre-sign Expect-header removal, version-aware reads, transport SHA-256
verification, streaming downloads to new application-owned local files,
and allowlisted secret-safe server error classifications.

The staged B2 processed-artifact repository retains the existing canonical
analytical manifests and codecs. A storage-specific `.publication.json`
completion descriptor is published last and pins exact artifact and manifest
object versions, byte sizes, and SHA-256 hashes.

A publication descriptor is not a distributed lock and does not provide an
HF-style repository-wide snapshot. Publication requires externally enforced
single-writer ownership of the exact artifact identity. Referenced object
versions must remain available; lifecycle cleanup must not remove them.

Incomplete publications are not importable. Compatible interrupted
publications may be resumed; conflicting or unverifiable existing objects
must not be silently overwritten.

The staged Backblaze B2 transport and verified processed-artifact repository
are implemented separately from the active Hugging Face production path.
The shared PostgreSQL importer now has backend-neutral storage ownership
and an explicit B2 import entry point. B2 imports retain endpoint, bucket,
and exact publication/object versions; they never fabricate an HF commit.

The local Fetch UI supports explicit HF and B2 range imports through one
application-owned runtime. HF remains the configured default unless changed.

The checked-in scheduled workflow explicitly selects Backblaze B2.
Verified seed/history migration, production writer coordination, and
controlled live cutover remain required.
Local importer tests do not establish completed cutover.

Production readiness still requires executed seed/history verification,
external writer coordination, and controlled live handoff.
Keep HF dependencies, source history, and explicit rollback credentials
until the rollback window is deliberately closed.
<!-- l2shock:b2-publication-foundation:end -->

<!-- l2shock:b2-import-boundary:start -->
### Staged B2 local import boundary

The shared importer accepts verified backend-neutral storage ownership.
B2 imports retain the actual endpoint, bucket, completion descriptor,
publication identity, and version-pinned artifact/manifest references.
Their legacy HF repository-revision field is null. A previous HF revision
is retained under explicitly HF-specific legacy metadata.

Verified downloads finish before the transactional download/import helper
opens its PostgreSQL session. Existing analytical channels are persisted
unchanged through the shared repositories.

Remote price imports acquire source-hour admission before quality decoding
and analytical persistence. Source-metadata reconciliation retains its own
transaction-lock requirement.

Explicit local B2 range-runtime and Fetch UI integration are implemented.
The checked-in scheduled workflow also selects B2. These implementation
facts do not establish executed seed/history handoff for every production
chain. HF remains available for explicit rollback.
<!-- l2shock:b2-import-boundary:end -->

<!-- l2shock:b2-range-runtime:start -->
### Explicit B2 Fetch UI and range-runtime integration

The shared remote-import runtime supports explicit HF and B2 execution.
The Fetch workflow selector exposes remote_hf_import, remote_b2_import,
and local_fetch_processing. The example local default remains HF unless
explicitly changed.

One application-owned remote runtime is visible to polling and shutdown.
An idle backend may be changed only while shared admission is free.
Active or finalizing work cannot be replaced. Configuration failure leaves
the previous singleton intact and never silently selects another backend.

B2 has no repository-wide commit revision. Each artifact resolves one
completion descriptor and downloads its exact version-pinned publication.
Successful items retain endpoint, bucket, publication, and object-version
ownership. Missing completion descriptors remain missing; broken pinned
publications and transport failures remain errors.

Transport cleanup finishes before PostgreSQL import begins. Repeated task
cancellation retains operation ownership until the synchronous worker exits.
Stop or shutdown before execution does not start repository work.
UI revision labels also support HF operations stopped before revision pinning.

The checked-in scheduled workflow selects B2. This source configuration
does not establish audited seed/history handoff or complete live readiness
for every production chain. HF remains an explicit rollback option.
<!-- l2shock:b2-range-runtime:end -->
<!-- l2shock:b2-worker-integration:start -->
### Explicit B2 worker execution and frontier integration

The shared remote worker supports explicit hugging_face and backblaze_b2
selection. HF remains the CLI default; the checked-in scheduled workflow
explicitly selects backblaze_b2.

l2shock/remote/b2_worker_repository.py scopes B2 access to one component
chain and preset. Each inspection generation retains one completion
reference, or absence, per key. Pinned publication failures remain errors;
reads never fall back to a newer current object.

The existing frontier, immediate-predecessor checkpoint, acquisition,
headless processing, invalid-hour, and price-repair policies are shared.
B2 worker results retain real endpoint, bucket, descriptor, and object
version ownership and contain no fabricated HF revisions.

B2 execution requires externally enforced single-writer ownership.
--b2-single-writer-confirmed acknowledges that ownership; it is not a lock.
The workflow serializes each component chain. Local migration tools and
other publishers remain outside that GitHub concurrency protection and
must not overlap its destination writes.

The caller owns the object-store lifetime. Joined synchronous work finishes
before cancellation can close the transport or temporary workspace.

Executed seed/history verification, final writer handoff, and live new-hour
worker validation remain deployment gates. Backend selection in a workflow
file is not proof those gates have been completed.
<!-- l2shock:b2-worker-integration:end -->

<!-- l2shock:b2-seed-migration:start -->
### Verified B2 seed/history migration tooling

l2shock/remote/b2_migration.py migrates an explicit bounded processed-artifact
plan from one pinned immutable HF commit, or from canonical verified local
artifact/manifest pairs, through the existing B2 processed-artifact repository.

Canonical manifests, analytical encoded channels, analytical hashes,
checkpoint bytes and identities, presets, and producer metadata are preserved.
The B2 transport container receives its own transport hash and object versions.

The tool verifies terminal L2 checkpoints before destination writes unless
--history-only is explicitly selected. History-only copying does not establish
checkpoint-chain readiness. Missing artifacts remain explicit and produce a
non-success CLI result. Conflicts and broken pinned publications stop the run.

Each successful item is recorded only after exact version-pinned B2 read-back.
The exclusive JSONL journal is flushed and fsynced. An incomplete journal or
publication intent alone is not proof of success. Recovery reruns the same
pinned source plan and verifies idempotent existing publications.

External destination single-writer ownership remains mandatory.
--b2-single-writer-confirmed acknowledges that exclusion; it is not a lock.
Migration and production publishers must not write overlapping B2 keys.

tests/test_remote_b2_worker.py exercises migration identity, terminal
checkpoint gates, pinned read-back, journal behavior, new-hour execution,
predecessor continuation, invalid-hour checkpoint suppression, price repair,
and interrupted completion-publication recovery.

Offline tests are not live service verification. The checked-in scheduled
workflow selects B2, but executed and audited seed migration and final writer
handoff remain deployment requirements. HF history and rollback credentials
are retained. CLI failure output does not print arbitrary exception details.
<!-- l2shock:b2-seed-migration:end -->
