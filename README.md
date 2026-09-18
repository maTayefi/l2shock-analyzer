# L2 Liquidity Shock Analyzer

Local historical order-book research application for reconstructing
CryptoHFTData L2 feeds, deriving compact liquidity observations, detecting
multi-scale Liquidity Movements, and displaying synchronized Binance perpetual
price and liquidity charts.

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
- Mandatory price source: Binance USD-M USDT perpetual trades.
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
last valid reconstructed state at or before each 1-second bucket end
    v
depth-band Bid Liquidity and Ask Liquidity
    v
eligible-market aggregation
    v
compact versioned hourly PostgreSQL blocks
    v
on-demand timeframe aggregation
    v
Liquidity Movement detection, ranking, and visualization
```
## Default remote preprocessing profile

The preferred production workflow is remote preprocessing through:

```text
CryptoHFTData
    ->
GitHub Actions standard Linux runner
    ->
shared l2shock acquisition/replay/liquidity/price engine
    ->
private Hugging Face dataset
    ->
local verified importer
    ->
local PostgreSQL
    ->
local Analysis and NiceGUI
```

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

The local verified Hugging Face importer consumes one artifact pair from one
full immutable dataset commit SHA.

The Fetch tab defaults to the `Remote HF Import` workflow profile. A remote
range import resolves one full Hugging Face revision and uses that same commit
SHA for every artifact in the operation.

The required remote artifact universe per selected UTC hour is:

```text
BTC:
    Binance Futures BTCUSDT component L2
    OKX Futures BTC-USDT-SWAP component L2
    Binance Futures BTCUSDT price

ETH:
    Binance Futures ETHUSDT component L2
    OKX Futures ETH-USDT-SWAP component L2
    Binance Futures ETHUSDT price
```

The component L2 keys use the exact existing single-market preset hashes for
the selected depth band. They do not use the aggregate Binance+OKX preset hash.

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

The complete workspace is deleted after compact results have been successfully
published by the later Hugging Face orchestration layer.

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
acquisition adapter, headless processors, and Hugging Face repository adapter.
It does not add another downloader or analytical implementation.

The worker supports these initial chains:

```text
binance_futures / BTCUSDT:
    L2 orderbook + Binance trade price

binance_futures / ETHUSDT:
    L2 orderbook + Binance trade price

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




Scheduled remote processing has two deployment gates:

```text
L2SHOCK_REMOTE_PROCESSING_ENABLED=true
    enables scheduled remote processing

L2SHOCK_BINANCE_SEEDS_READY=true
    admits Binance BTC and ETH into scheduled processing
```

When remote processing is enabled but the Binance seed gate is not exactly
lowercase `true`, scheduled runs include only the two OKX chains.

Manual workflow dispatch may still select one Binance chain for controlled
snapshot bootstrap or checkpoint validation.

Do not set `L2SHOCK_BINANCE_SEEDS_READY=true` until both Binance chains own
verified L2 artifacts containing usable output checkpoints.

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
Required GitHub configuration is:

```text
Actions secret:
    HF_TOKEN

Actions variable:
    L2SHOCK_HF_REPO_ID

Optional Actions variables:
    L2SHOCK_HF_REVISION
    L2SHOCK_DEPTH_LOWER
    L2SHOCK_DEPTH_UPPER
```

The workflow serializes each venue/instrument chain independently while
allowing different chains to run concurrently.

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

The first remote scope is:

```text
Binance Futures BTCUSDT orderbook
Binance Futures BTCUSDT trades
Binance Futures ETHUSDT orderbook
Binance Futures ETHUSDT trades
OKX Futures BTC-USDT-SWAP orderbook
OKX Futures ETH-USDT-SWAP orderbook
```

Bybit and Bitget remain postponed. They must not be enabled in the remote
workflow until their snapshot/bootstrap and sequence contracts are proven and
their local adapters pass the same replay tests as Binance and OKX.

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

Bybit remains excluded from the GitHub remote-worker matrix, Hugging Face
publication/import planning, and multi-market aggregate presets until the
remaining integration batches are complete.

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

### Bybit diagnostic evidence gate

Bybit production support remains fail-closed while a dedicated GitHub Actions
diagnostic collects bounded event-level evidence.

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

This batch does not yet add Bybit to the GitHub remote-worker matrix, Hugging
Face publication/import planning, or a Binance+OKX+Bybit aggregate preset.

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

Binance Futures trades remain the sole price source for chart OHLC and price
filtering.

OKX trade files are not included in normal production acquisition merely
because their schema is available. The OKX integration currently contributes
single-market reconstructed L2 liquidity only.

Manual Fetch and Automatic Fetch use the same six-file source universe.

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

A degraded aggregate keeps the exact sum of the markets that did contribute.
It also records:

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


### Strict aggregate analysis loading

Multi-market presets are resolved into their immutable component single-market
preset identities during verified Analysis loading.

Each component row is loaded and decoded independently through the normal
single-market PostgreSQL verification boundary.

At every exact UTC second, all expected markets must contribute valid Bid and
Ask Liquidity. For the Binance + OKX aggregate preset, the contributing market
set must remain exactly Binance Futures plus OKX Futures throughout the
effective Analysis range.

A missing or invalid expected market is never represented as zero, never
forward-filled, and never silently omitted from the aggregate.

If fewer than all expected markets contribute at any second, verified Analysis
loading fails with a market-coverage error. The user must process the missing
component data or narrow the requested range.

This prevents changes such as Binance-only liquidity followed by Binance+OKX
liquidity from producing artificial Bid, Ask, Total, Delta, or Imbalance steps
that could be misclassified as Liquidity Movements.

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
- price-filter eligibility;
- timeline segmentation;
- LM boundaries or confirmation;
- LM candidate inclusion;
- normalization;
- percentile or modified-Z calculation;
- ranking;
- cross-timeframe support;
- highlight ownership;
- event plotting location.

Semantic changes must never be made silently.

Non-semantic changes include implementation, performance, UI, logging,
refactoring, cancellation, error handling, testing, and storage optimization
only when analytical output remains mathematically equivalent.


LM ranking V2 is a semantic change. The former terminal-endpoint-only
extremeness evidence was replaced by an equal-weight mean of directional
starting-pivot extremeness and terminal-extremum extremeness.

Candidate detection, LM start/end ownership, confirmation ownership, Top-N
recall, percentile calculations, and modified-Z calculations remain unchanged.

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
Completeness continues to be derived from all six required durable source
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

Calendar handoff therefore follows the same strict full-market coverage policy
as verified Analysis loading.

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

Version 1 fixed-duration aggregation supports:

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

Calendar month timeframes (`1M`, `3M`, `6M`) are deferred until a separate
calendar-aware ownership contract is implemented. A month must never be
silently represented as a fixed number of seconds.

User-selected analysis endpoints are interpreted as a closed continuous-time
range `[m, n]`. For active timeframe `T`, the effective bar-aligned loading
range is:

```text
snapped_start = floor(m, T)
snapped_end   = floor(n, T) + T

effective range = [snapped_start, snapped_end)
```

Therefore both timeframe bars containing the selected endpoints are included.
An end time exactly on a bar boundary includes the bar beginning at that
boundary.

The automatic chart timeframe is the finest supported timeframe whose snapped
bar count does not exceed the configured maximum chart bars. If no supported
timeframe fits, the coarsest supported timeframe is selected and the UI may
warn that the target was exceeded.

L2 liquidity is a reconstructed state metric. A larger L2 bar owns the final
one-second L2 state in its interval.

L2 aggregate quality is:

```text
all seconds valid and endpoint valid:
    VALID

endpoint valid but an earlier second is invalid or missing:
    DEGRADED

endpoint invalid or missing:
    INVALID
```

Any invalid or missing one-second L2 observation creates an explicit hard
discontinuity. Later LM detection and timeframe analysis must not bridge that
discontinuity merely because an aggregate endpoint is numerically available.

Real traded price uses standard OHLC aggregation over valid one-second trade
candles:

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

Price aggregate quality is:

```text
all seconds contain valid real-trade candles:
    VALID

some but not all seconds contain valid real-trade candles:
    DEGRADED

no second contains a valid real-trade candle:
    INVALID
```

Invalid and missing lower-level price observations remain explicit coverage
facts. They are not forward-filled. Any such observation creates a hard
discontinuity for LM segmentation even when a larger degraded OHLC bar can be
constructed from other real trades in that interval.

Fixed-duration bucket alignment is UTC internally. User-visible timestamps are
converted to the configured display timezone without changing bucket identity.


### Verified aligned analysis datasets

Analysis loading reads compact L2 and price rows through their normal verified
PostgreSQL repository boundaries.

Every loaded row is checked for:

```text
row identity
combined content SHA-256
per-channel envelope and payload hashes
Arrow schema and metadata
exact 3,600-slot ownership
quality-summary consistency
typed source provenance
```

The loader decodes authoritative one-second observations and aligns L2 and
price by exact UTC second identity.

A missing persisted L2 or price hour is not silently omitted from coverage.
Its absent seconds become explicit invalid/missing coverage during aggregation.

Activity and chart resolutions are built independently from the same verified
one-second inputs:

```text
activity timeframe:
    user-selected from 1s, 5s, 10s, 15s, 30s

chart timeframe:
    automatically selected from the fixed-duration registry according to the
    configured maximum chart-bar count
```

Each resolution applies its own permissive closed-endpoint snapping. Therefore
activity and chart series may have different aligned outer ranges while
remaining owned by one analysis request.

For every aligned aggregated bar, the dataset retains:

```text
exact L2 endpoint values
exact real-trade aggregate OHLC
price-filter eligibility
display-only clipped OHLC
L2 coverage diagnostics
price coverage diagnostics
hard-discontinuity state
explicit discontinuity reasons
```

A bar belongs to a continuous analysis core only when:

```text
true price OHLC intersects the active price bounds
and
price coverage contains no invalid/missing lower-level second
and
L2 coverage contains no invalid/missing lower-level second
and
the L2 endpoint owns valid Bid and Ask Liquidity
```

A larger degraded bar may remain visible for diagnostics, but it cannot hide a
lower-level discontinuity or join two LM candidate runs.

Price-excluded periods remain explicit discontinuity runs. They are not removed
and concatenated into a false continuous timeline.

Detection context may include a configured number of adjacent valid
price-excluded bars before or after one core segment. Context:

```text
cannot cross a hard L2 or price boundary
cannot include another eligible core segment
cannot merge two independently eligible segments
```

Each dataset receives a deterministic analysis ID. Its canonical identity
includes:

```text
base
data-preset hash
closed requested UTC range
activity timeframe
automatic chart timeframe and bar budget
price bounds
context counts
software/schema version
expected hourly coverage
exact persisted L2 content hashes
exact persisted price content hashes
```

Changing display colors, panel visibility, or temporary chart navigation does
not change this analysis identity.


### Price source identity

Price charts and price filtering use real Binance USD-M USDT perpetual trades:

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


### Price-filter semantics

The scan range and price filter are separate concepts.

The scan range defines the requested time window.

A price bar is price-filter eligible when its true OHLC interval intersects the
active price bounds:

```text
bar.high >= min_price
and
bar.low <= max_price
```

Close-only eligibility is prohibited.

An eligible candle remains visible, but its display-only OHLC may be clipped to
the selected price bounds. Raw Binance OHLC must remain unchanged for
provenance, export, diagnostics, and any calculation requiring real values.

A candle whose complete OHLC range lies outside the active bounds is excluded
from the filtered analysis core.

The user price filter never removes order-book levels and never changes the
depth-band liquidity calculation.

### Segmented filtered timeline

Excluded price periods are explicit time discontinuities.

Retained bars must not be concatenated around an excluded interval. Timeframe
aggregation and LM detection must not bridge:

- excluded price-filter runs;
- invalid L2 runs;
- invalid price runs;
- non-contiguous timestamp gaps.

Charts must mark skipped periods with a visible discontinuity indicator and
report the skipped elapsed duration.

### Price-filter context

A configurable number of valid observations immediately before and after a
price-filter-eligible segment may be retained as detection context.

Default:

```text
context bars before = 3
context bars after  = 3
```

Context may extend the edge of one eligible segment. It must never merge two
eligible segments separated by excluded or invalid observations.

Where useful, an LM result may record both:

```text
full_start / full_end
in_range_start / in_range_end
```

### Data-quality states

Derived observations use:

```text
VALID
DEGRADED
INVALID
```

Invalid observations are never converted into valid observations by
forward-fill.

A valid reconstructed book may retain its last state through a quiet bucket,
but only while initialization and sequence continuity remain proven.

An LM:

- cannot start or end on an invalid observation;
- cannot cross an invalid run;
- cannot cross a filtered-time discontinuity;
- may overlap degraded observations only while retaining explicit quality
  diagnostics.

Warnings do not automatically block analysis of other valid segments.

A persistent red warning region is required when:

```text
L2 is invalid or unavailable for more than 60 consecutive seconds.
```

A persistent red warning region is required when:

```text
real Binance traded price is invalid or unavailable for more than
3 consecutive minutes.
```

### Liquidity Movement definition

UI term:

```text
Liquidity Movement (LM)
```

An LM is a directional movement in one liquidity metric from a starting pivot
to a terminal extremum.

It becomes confirmed when a subsequent opposite movement retraces at least the
configured fraction of the current movement height.

Default confirmation retracement:

```text
20%
```

The LM endpoint is the extremum bar, not the later confirmation bar.

Because analysis is post-scan, an unresolved right-edge extremum may be retained
with:

```text
terminal_offline = true
```

It must remain visibly identified as an offline terminal endpoint.


### Segment-scoped LM candidate detection

Liquidity Movement detection operates independently inside every verified
analysis segment.

The detector may use the segment's explicitly owned context bars to determine a
movement's full extent, but a retained candidate must overlap the segment's
core eligible range.

A candidate must never cross:

```text
price-filter discontinuity
invalid price
invalid L2
missing price or L2 coverage
non-contiguous timestamp boundary
unavailable selected metric
```

For an upward movement, the current endpoint is the highest observed metric
value after its starting pivot. For a downward movement, the current endpoint
is the lowest observed metric value after its starting pivot.

An opposite movement confirms the current candidate when:

```text
opposite_retracement / current_absolute_height
>=
confirmation_retracement_fraction
```

The default confirmation fraction is:

```text
0.20
```

The candidate endpoint remains the extremum bar. The later bar satisfying the
confirmation threshold is recorded separately as the confirmation bar.

Equal extremum values use the earliest extremum bar. This deterministic rule
prevents repeated equal values from shifting a candidate endpoint forward
without changing its mathematical height.

An unresolved movement reaching the right edge of its owned analysis segment
may be retained with:

```text
terminal_offline = true
```

An unresolved movement interrupted by invalid or unavailable metric data is not
reclassified as an offline terminal candidate.

Each candidate records:

```text
metric
timeframe
direction
segment identity
full start and end
in-range start and end
confirmation bar, when present
absolute height
relative height
inclusive bar count
sharpness
adverse-move count
adverse-move total and maximum
adverse-move fractions
degraded-bar count
offline-terminal state
```

Candidate detection does not calculate population percentiles, modified
Z-scores, Top-N selection, final priority ordering, or visualization alpha.
Those belong to the later ranking batch.


### Fundamental LM measurements

For one candidate:

```text
absolute_height = abs(end_value - start_value)

relative_height =
    absolute_height / (scan_max - scan_min)

bars =
    end_bar - start_bar + 1

sharpness =
    relative_height / sqrt(bars)
```

Internally use the term `height`. Do not interpret “longness” as duration.

### Ranking populations

Every ranking population is isolated by:

```text
liquidity metric
x analytical timeframe
x direction
```

For example, Ask/1s/Up candidates do not compete with Bid/1s/Up,
Ask/chart-timeframe/Up, or Ask/1s/Down candidates.

### Primary LM evidence

Primary ranking evidence is:

1. relative-height percentile;
2. relative-height positive-tail modified-Z;
3. sharpness percentile;
4. sharpness positive-tail modified-Z.

Percentiles use midpoint tie ownership. For a non-singleton population:

```text
percentile =
    (
        number of population values below the candidate
        +
        (number equal to the candidate - 1) / 2
    )
    /
    (population size - 1)
```

A singleton population receives percentile `1`.

Positive-tail modified-Z evidence is:

```text
median = population median

MAD =
    median(abs(value - median))

modified_z =
    0.6744897501960817
    * (value - median)
    / MAD

positive_tail_modified_z =
    max(0, modified_z)

normalized_positive_tail_modified_z =
    positive_tail_modified_z
    /
    (1 + positive_tail_modified_z)
```

If `MAD = 0`, values at or below the median receive zero positive-tail
evidence. A value above the median receives normalized positive-tail evidence
`1`. Its raw modified-Z diagnostic remains zero because no finite modified-Z
exists in that case.

Height and sharpness statistical evidence are each:

```text
evidence =
    (
        percentile
        +
        normalized_positive_tail_modified_z
    )
    / 2
```

The primary candidate pool is the union of:

```text
Top N by height evidence
Top N by sharpness evidence
```

Exactly Top N candidates are selected from each ordering before union.
Statistical ties use deterministic candidate identity ordering rather than
expanding the requested Top-N count.

This recall guard prevents a very large sufficiently sharp movement or a very
sharp sufficiently large movement from being lost merely because one combined
score ranked it lower.

### Secondary LM evidence

Secondary ordering evidence is:

1. equal-weight LM boundary extremeness;
2. retracement magnitude quality;
3. retracement count quality.

LM boundary extremeness gives the starting pivot and terminal extremum
identical importance. It is the arithmetic mean of directional start
extremeness and directional end extremeness.

LM boundary extremeness uses the complete scan-level range of the selected
metric and timeframe.

For an upward LM:

start_extremeness =
    (scan_max - start_value) / (scan_max - scan_min)

end_extremeness =
    (end_value - scan_min) / (scan_max - scan_min)

For a downward LM:

start_extremeness =
    (start_value - scan_min) / (scan_max - scan_min)

end_extremeness =
    (scan_max - end_value) / (scan_max - scan_min)

For both directions:

boundary_extremeness =
    (start_extremeness + end_extremeness) / 2

Start and end therefore receive exactly equal importance. The existing
extremeness priority is allocated to this equal-weight mean; it is not applied
in full twice.

Adverse movement between the candidate start and extremum represents internal
retracement/noise.

Retracement magnitude quality is:

```text
1
/
(1 + adverse_move_total_fraction)
```

Retracement count quality is:

```text
1
-
(
    adverse_move_count
    /
    (candidate_bars - 1)
)
```

Both quality values lie inside `[0, 1]`, where a larger value means a cleaner
directional movement.

The approved default priority values are:

```text
height priority                 = 0.35
sharpness priority              = 0.30
boundary extremeness priority   = 0.20
retracement magnitude priority  = 0.10
retracement count priority      = 0.05
```
Within the boundary-extremeness allocation:

start boundary share = 0.10
end boundary share   = 0.10

```text
The configuration key remains `priority_endpoint_extremeness` for the current
configuration schema. In ranking algorithm V2 it controls the combined
equal-weight boundary-extremeness factor. It no longer scores only the terminal
endpoint.
```

Primary evidence is normalized using only the height and sharpness priorities.
Secondary evidence is normalized using only the three secondary priorities.

Final ranking is priority-aware and lexicographic:

```text
primary evidence
then secondary evidence
then component evidence
then deterministic candidate identity
```

A flat weighted evidence value may be retained as a diagnostic, but it does not
own final ranking. Secondary evidence therefore cannot bury a candidate with
stronger primary evidence.


### LM analysis execution and deterministic result identity

Liquidity Movement execution starts from one verified aligned analysis dataset.

For each unique analytical timeframe owned by that dataset, the executor runs
the same detector for every configured liquidity metric:

```text
Bid Liquidity
Ask Liquidity
Total Liquidity
Bid-Ask Imbalance
```

Activity and chart resolutions are separate when their timeframe labels differ.
If both roles resolve to the same timeframe, that timeframe is executed once.
The system must not duplicate candidates merely because one series has both
activity and chart presentation ownership.

For each metric and timeframe, scan bounds are calculated from the complete
usable selected-metric series. They are not inferred only from detected
candidate endpoints.

Streams with fewer than two usable values or with zero metric range produce an
explicit empty result slice. They do not fail the complete analysis.

The executor then performs:

```text
segment-scoped candidate detection
-> explicit metric/timeframe scan bounds
-> independent metric/timeframe/direction population ranking
-> Top-N height/sharpness recall union
-> immutable result slices
```

Every execution receives a deterministic SHA-256 identity derived from:

```text
aligned dataset analysis ID
selected metrics
unique analytical timeframe labels
detector schema and algorithm versions
confirmation-retracement configuration
detector Decimal precision
ranking schema and algorithm versions
Top-N configuration
ranking priorities
ranking Decimal precision
analysis-execution schema and algorithm versions
```

The execution identity excludes:

```text
chart colors
visibility toggles
table sort order
selected table row
crosshair position
zoom and navigation state
progress callbacks
cache state
```

Changing a semantic detector or ranking setting creates a different execution
identity.

Completed immutable results may be stored in a bounded process-local LRU cache.
A cache hit reuses only an exact matching execution identity. Cancelled or
failed executions must never publish partial results into the cache.

Progress callbacks are operational diagnostics. A progress-rendering failure
does not alter detection or ranking truth.


### Application-owned analysis runtime

Verified dataset loading, compact-channel decoding, timeframe aggregation,
Liquidity Movement detection, and population ranking are synchronous analytical
operations.

The application executes them in a worker thread so the NiceGUI event loop
remains responsive.

Analysis shares one process-wide operation admission boundary with:

```text
Manual Fetch
Manual Processing
Manual Analysis
```

Only one of those operations may run at a time.

The runtime owns:

```text
operation UUID
cooperative cancellation Event
latest progress
latest immutable completed result
latest secret-safe error
bounded process-local analysis cache
```

Database sessions are created and closed inside the worker thread which uses
them. ORM objects never cross the worker/event-loop boundary.

Stop Analysis sets the thread-safe cancellation event. The synchronous
executor checks that event between analytical slices and before cache
publication.

Native cancellation of the owning asyncio task also sets the cooperative event
and waits for the worker boundary. Cancelling an asyncio wrapper is never
treated as if it forcibly terminated the Python worker thread.

Application shutdown follows:

```text
raise admission barrier
-> stop Manual Fetch
-> stop Manual Processing
-> stop Manual Analysis
-> cancel unrelated tracked tasks
-> dispose SQLAlchemy engine
-> stop NiceGUI
```

A cancelled or failed analysis never publishes a partial result into the cache.

Completed analysis results are process-local presentation/runtime objects. They
are not PostgreSQL rows, raw source storage, or database backups.


### Functional Analysis controls and result table

The Analysis tab constructs one immutable verified-analysis request from:

```text
base asset
enabled data-preset hash
closed local-time start/end inputs
activity timeframe
maximum chart-bar budget
optional minimum and maximum price bounds
price-filter context counts
LM confirmation fraction
Top-N height count
Top-N sharpness count
```

User-local datetimes are converted strictly through the configured IANA
timezone. Ambiguous or nonexistent local times are rejected.

Only enabled persisted data presets are selectable. Selecting a preset does not
edit or reinterpret its semantic identity.

Run Analysis uses the application-owned analysis runtime. Manual Fetch, Manual
Processing, and Manual Analysis remain mutually exclusive through the shared
process operation boundary.

The first result UI presents the selected candidate union:

```text
Top N by height evidence
union
Top N by sharpness evidence
```

for every independent:

```text
metric
x timeframe
x direction
```

population.

The result table retains sortable diagnostics including:

```text
candidate start and extremum times
confirmation time
offline-terminal state
absolute and relative height
bar count
sharpness
height percentile and modified-Z evidence
sharpness percentile and modified-Z evidence
primary and secondary ranking evidence
directional start extremeness
directional end extremeness
equal-weight boundary extremeness
retracement magnitude quality
retracement count quality
quality diagnostics
selection flags
```

Table values are presentation copies. Exact analytical Decimal values remain
owned by the immutable process-local analysis result.

The Analysis UI includes synchronized chart rendering, selected-candidate
navigation, browser render acknowledgement, chart-timeframe rebuilding, and
JSON/PNG/SVG export.

All chart controls remain presentation-only unless an explicit chart timeframe
is selected. An explicit chart timeframe rebuilds the verified analytical
series and therefore owns a new deterministic Analysis identity.


### Synchronized Analysis chart workspace

The first Analysis chart workspace uses one ECharts instance containing five
vertically stacked grids:

```text
Binance perpetual price candlesticks
Bid Liquidity
Ask Liquidity
Total Liquidity
Bid-Ask Imbalance
```

One ECharts instance owns:

```text
one canonical chart-timeframe category identity
linked x-axis pointers
shared inside zoom
shared slider zoom
shared horizontal navigation
```

The price panel uses display-only clipped OHLC when active price bounds require
clipping. Exact real Binance OHLC remains unchanged in the immutable analysis
result.

The chart displays chart-timeframe core-eligible bars. Invalid or
price-filter-excluded runs are compressed from the visible category sequence,
but every compression boundary receives an explicit timeline-jump marker with:

```text
skipped elapsed seconds
discontinuity reasons
persistent-warning state
```

A timeline jump must never be presented as ordinary contiguous elapsed time.

Long data warnings use the configured project thresholds:

```text
L2 invalid/unavailable for more than 60 seconds
real traded price invalid/unavailable for more than 180 seconds
```

Liquidity Movement highlight visibility is presentation-only.

The initial visibility controls independently own:

```text
Price destination
liquidity-subplot destination
activity-timeframe candidates
chart-timeframe candidates
Top-N height selection
Top-N sharpness selection
Bid / Ask / Total / Imbalance metric layers
```

Changing those controls does not change:

```text
candidate detection
population statistics
ranking
Top-N selection
analysis identity
stored data
```

Individual candidate opacity uses the ranking diagnostic
`priority_weighted_evidence` and the configured linear alpha range.

Same-direction overlapping candidates accumulate by alpha composition.
Combined upward/downward opacity is bounded by the configured accumulated-alpha
cap.

Cross-timeframe overlap remains visualization aggregation only. Candidates from
different timeframe populations never compete statistically merely because
their chart highlights overlap.

The chart workspace now includes:

```text
browser render acknowledgement
stale-publication rejection
custom gapped Price crosshair
vertical-only liquidity-panel cursor lines
table-row navigation
selected-LM emphasis
explicit chart-timeframe rebuilding
timestamp-owned left-edge restoration
deterministic JSON export
browser-owned PNG and SVG export
```

These capabilities do not alter persisted data or LM ranking truth.


### Chart publication and interaction ownership

Every complete Analysis chart option receives a unique hidden render identity.

A chart generation is considered committed only after the browser confirms:

```text
the expected render-token series exists
the expected category count exists
the ECharts instance has a usable width and height
the Python widget has not been superseded by a later publication
```

Updating only NiceGUI's stored component property is insufficient because the
live ECharts instance may retain removed series or graphics.

Updating only the live ECharts instance is also insufficient because a later
NiceGUI component update may replay an older stored property.

Therefore publication requires:

```text
NiceGUI/Vue complete options property
+
ECharts setOption(notMerge=true)
+
browser acknowledgement
```

Presentation-only highlight changes preserve the current percentage-based
dataZoom viewport when the same completed Analysis result remains the owner.

The custom Analysis crosshair is browser-side and does not round-trip mousemove
events through Python.

On the Price panel it uses four line segments with an approximately 50-pixel
gap around the pointer:

```text
left horizontal
right horizontal
upper vertical
lower vertical
```

On Bid, Ask, Total, and Imbalance panels it uses synchronized vertical-only
lines.

Native ECharts axis-pointer lines remain transparent while their category and
tooltip ownership remains available.

Clicking a selected-LM table row navigates both chart dataZoom components to
the candidate's compressed chart-category interval. Navigation is accepted only
when:

```text
the current Analysis result owns the row
the browser-acknowledged chart generation is still current
the candidate overlaps visible chart-timeframe core bars
```

Table navigation, crosshair graphics, zoom state, and visibility controls are
presentation-only. They never change:

```text
candidate detection
ranking populations
Top-N selection
analysis identity
stored data
```


### Selected-LM emphasis and Analysis export

Clicking a selected Liquidity Movement table row adds an amber focus region to
the candidate interval in all five synchronized chart panels.

Selected focus is presentation-only. It does not change:

```text
candidate boundaries
ranking evidence
Top-N selection
analysis identity
stored analytical data
```

The selected focus owner is retained across presentation-only highlight
rerenders while the same immutable Analysis result continues to own the chart.

Completed Analysis results can be exported as deterministic JSON.

The JSON export contains:

```text
analysis and dataset identities
dataset provenance
execution schema and algorithm versions
exact semantic configuration
metric/timeframe slices
scan bounds
all candidates
all rankings
selected-candidate identities
exact analytical decimals as canonical strings
display timezone identity
```

The JSON export excludes:

```text
browser render tokens
zoom state
crosshair state
selected table-row presentation state
temporary chart visibility controls
```

PNG and SVG exports are produced from the exact browser-acknowledged chart
generation.

Immediately before image capture, export verifies that:

```text
the current Analysis result still owns the chart
the acknowledged render token is unchanged
the browser ECharts instance still contains that token
```

Image export clones the live chart option into an off-screen ECharts instance.
Transient custom-crosshair graphics are removed from the clone. Selected-LM
focus regions remain because they are deliberate series-owned presentation
state.

PNG uses a Canvas export clone. SVG uses an SVG-renderer export clone.

Exported Analysis JSON, PNG, and SVG files are research artifacts. They are not
database backups.


### Explicit chart-timeframe rebuilding

The chart timeframe may be:

```text
automatic
or
one explicitly selected registered fixed-duration timeframe
```

Automatic selection chooses the finest registered fixed timeframe whose
permissively snapped chart range fits the configured maximum automatic chart
bar count.

An explicit chart-timeframe selection is analytical input. It does not resample
or interpolate the existing browser chart.

Changing it performs:

```text
new immutable AnalysisDatasetRequest
-> verified PostgreSQL compact-row loading
-> fixed-timeframe aggregation
-> price-filter segmentation
-> chart-timeframe LM detection
-> isolated population ranking
-> new deterministic dataset identity
-> new deterministic execution identity
-> complete acknowledged chart publication
```

The explicit selection policy and requested chart timeframe are included in
dataset provenance.

Activity-timeframe and chart-timeframe LM populations remain independent. If
the explicit chart timeframe equals the activity timeframe, the execution
engine continues to process that unique series only once.

Before a chart-timeframe rebuild begins, the application may capture the
currently acknowledged chart's:

```text
visible left-edge UTC timestamp
visible real elapsed duration
source chart-timeframe duration
```

After the new result is browser-acknowledged, it maps the old UTC left edge to
the nearest visible category in the new compressed timeline and restores an
approximately equal elapsed duration.

Timestamp-based restoration is accepted only when:

```text
the old chart generation was acknowledged
the captured viewport belongs to the exact request being rebuilt
the new completed result owns that request
the new chart generation is acknowledged
the new chart still owns the expected render token
```

Filtered or invalid runs remain hard analytical discontinuities. Timestamp
mapping may move to the nearest visible category when the exact old edge is not
present, but it never recreates an excluded category or joins separate
analytical segments.

Calendar-month timeframes remain excluded from the fixed-duration registry.
They require a separate calendar-aware aggregation and identity contract.


### Multi-timeframe behavior

The stored one-second series may be aggregated into:

```text
1s
5s
10s
15s
30s
automatic chart timeframe
```

One shared detector implementation is applied to each derived series.

Rankings remain independent by metric, timeframe, and direction.

Cross-timeframe overlap is visualization aggregation only. It must not merge
statistical ranking populations.

A time/price region supported by several strong candidates may become more
visible through accumulated highlight alpha.

### Highlight behavior

Default direction colors:

```text
Upward LM   = blue
Downward LM = red
```

Individual alpha is score-driven, initially within approximately:

```text
0.05 to 0.18
```

Overlapping highlights may accumulate, but effective alpha should be capped
around `0.35-0.40` to preserve candle readability.

Visibility controls affect rendering only. They never change detection,
scoring, ranking, or stored data.

### Timezone contract

All persisted timestamps use timezone-aware UTC.

User inputs and all user-visible times use the configured IANA timezone.

Default:

```text
Asia/Tehran
```

Ambiguous or nonexistent DST local times must be rejected rather than silently
resolved.

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
LM parameters
Top-N
highlight opacity
price display filters
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
OKX Futures single market
Binance + OKX aggregate analysis identity
```

Unsupported or unrecognized market compositions remain read-only and must not
be reinterpreted through this editor.


#### Approved Preset CRUD market profiles

The Settings editor supports these exact market compositions:

```text
Binance Futures:
    BTCUSDT or ETHUSDT

OKX Futures:
    BTC-USDT-SWAP or ETH-USDT-SWAP

Binance + OKX Futures aggregate:
    both independently reconstructed component markets
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
partial sum for diagnostics. Verified Liquidity Movement Analysis rejects that
second whenever it overlaps the effective activity or chart range.

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

Every analysis result and export must include provenance sufficient to identify:

- base;
- scan bounds;
- timezone;
- price source;
- data preset hash;
- depth bounds;
- metric;
- activity timeframe;
- chart timeframe;
- LM settings;
- ranking settings;
- quality policy;
- software version;
- analysis schema version;
- source-hour identities.

JSON, PNG, and SVG exports must never be presented as database backups.

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
11. timeframe aggregation and filtering;
12. LM detection and ranking;
13. orchestration and caching;
14. Fetch and Settings UI;
15. Analysis UI;
16. synchronized charts;
17. exports and hardening.

Reconstruction and liquidity arithmetic must be proven before chart work begins.
