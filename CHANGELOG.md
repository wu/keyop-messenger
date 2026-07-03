## [1.24.0](https://github.com/wu/keyop-messenger/compare/v1.23.0...v1.24.0) (2026-07-02)

### Features

* **ephemeral:** stop reconnecting on non-retryable hub rejections via OnFatal ([d71a784](https://github.com/wu/keyop-messenger/commit/d71a784d5a046b419b48655bdd22d054ddaefb03))

### Bug Fixes

* **storage:** honor persisted subscriber offsets during compaction across restarts ([52d6920](https://github.com/wu/keyop-messenger/commit/52d6920b291a494def21e60019050794df1009fa))

## [1.23.0](https://github.com/wu/keyop-messenger/compare/v1.22.0...v1.23.0) (2026-07-01)

### ⚠ BREAKING CHANGES

* **storage:** the storage.max_channel_size_mb config option is removed and
replaced by storage.max_files. Disk-bounding is now on by default
(10 log files per channel, including the active one); set max_files: 0 to
restore the previous unbounded, pure consumption-based behavior.

### Features

* **audit:** log outbound forwards in addition to inbound ([23f12fe](https://github.com/wu/keyop-messenger/commit/23f12fef8b4764a5eedef2dd1889b915a5e6bceb))
* **audit:** record send→ack duration on outbound forwards ([c60be30](https://github.com/wu/keyop-messenger/commit/c60be30bb99aa2e1f0cb8519450397440af5b4ee))
* **federation:** prevent routing loops with a path-vector on the envelope ([8e40c2b](https://github.com/wu/keyop-messenger/commit/8e40c2bd153402c3e479b847a4a38d094bb32eca))
* **storage:** replace max_channel_size_mb with max_files (default 10) ([1b28ab5](https://github.com/wu/keyop-messenger/commit/1b28ab5dd445ed6c17da0d23f465b86f24534c2b))

### Bug Fixes

* **federation:** enforce MaxBatchBytes on hub ingest and ephemeral publish ([49f5d8f](https://github.com/wu/keyop-messenger/commit/49f5d8fd7ab7c19ab6bada957e078f2ebf72f1d1))
* **federation:** reap orphaned fedout- offset files on startup ([5562415](https://github.com/wu/keyop-messenger/commit/5562415177d770fc1e8615d7aa0c36fbd9fb4be0))

### Refactoring

* **federation:** remove dead AllowForward policy code ([2e9bd7c](https://github.com/wu/keyop-messenger/commit/2e9bd7c061c90cb7b92871b0ebf505baf2a22d96))

## [1.22.0](https://github.com/wu/keyop-messenger/compare/v1.21.0...v1.22.0) (2026-06-26)

### ⚠ BREAKING CHANGES

* **registry:** An unregistered payload_type is no longer delivered to
subscribers as map[string]any. Durable subscribers dead-letter such messages;
ephemeral subscribers skip them with a warning. registry.New() no longer takes
a logger argument.

### Features

* **registry:** drop support for unregistered payload types; dead-letter undecodable messages ([48dd495](https://github.com/wu/keyop-messenger/commit/48dd49562205c6d579481f55c2a30c543dce2c4e))

### Bug Fixes

* **federation:** validate inbound channel names to prevent path traversal ([d9fad8f](https://github.com/wu/keyop-messenger/commit/d9fad8fa6b02c8ab0fa58d9555df73702f708e46))
* **storage,federation:** sanitize subscriber IDs and peer CNs in offset paths ([f98292a](https://github.com/wu/keyop-messenger/commit/f98292aa130b0aef5b00c82269a40930190b6a97))
* **storage:** roll back partial writes to prevent log corruption and message loss ([4ebbe9c](https://github.com/wu/keyop-messenger/commit/4ebbe9c0fae25d91bf01589b235650c3adf11224))

## [1.21.0](https://github.com/wu/keyop-messenger/compare/v1.20.0...v1.21.0) (2026-06-26)

### Features

* add WindowCount to LatencyStage ([d6f7b7d](https://github.com/wu/keyop-messenger/commit/d6f7b7d4fee7e548fbd38ce7b1abba12eccec846))

## [1.20.0](https://github.com/wu/keyop-messenger/compare/v1.19.0...v1.20.0) (2026-06-26)

### Features

* add federation client->hub ack-RTT latency ([3d0bb7e](https://github.com/wu/keyop-messenger/commit/3d0bb7eecb55417932112751f5a9a9a4d8ca9cd5))
* add hub->peer subscribe-stream delivery RTT ([872276e](https://github.com/wu/keyop-messenger/commit/872276e7477f6b0e73a2d2e8be90473249a2b080))
* add per-stage latency aggregates to Stats ([217b282](https://github.com/wu/keyop-messenger/commit/217b28241f56265e2d683ddbb147c30678dc4869))
* add pre-summed Totals to Stats for golden-signal metrics ([f7f1559](https://github.com/wu/keyop-messenger/commit/f7f1559b3d7995499cae2818537479e424f68f4a))
* add recent p50/p90/p99 latency percentiles to Stats ([9c3eff9](https://github.com/wu/keyop-messenger/commit/9c3eff95a7a079fc48650af11c657df94ec9705e))
* count federation batch send failures (both directions) ([45ddd54](https://github.com/wu/keyop-messenger/commit/45ddd54c769e3fe17b05c16a7bd425c4e03ace42))
* **subscribers:** make the retry/backoff window configurable and per-subscriber ([876de80](https://github.com/wu/keyop-messenger/commit/876de80b448ec445fbdd29abcd5b6bdf70c82c0b))

## [1.19.0](https://github.com/wu/keyop-messenger/compare/v1.18.0...v1.19.0) (2026-06-25)

### Features

* **storage:** reserve dead-letter channels and bound their growth ([7e4d384](https://github.com/wu/keyop-messenger/commit/7e4d384798c07471edf592e9309349a2719d7043))

### Bug Fixes

* **federation:** make coordinator/reader close idempotent under concurrency ([e68ff4f](https://github.com/wu/keyop-messenger/commit/e68ff4f589c14ed4a444da7b7c1ab27502560c49))

### Refactoring

* remove unused fsnotify watcher and update design doc ([a30c3bd](https://github.com/wu/keyop-messenger/commit/a30c3bd24e5b06cc1a08b27719a69a10739bbe77))
* **storage:** drop advisory flock from the write path ([6f4fd10](https://github.com/wu/keyop-messenger/commit/6f4fd10e8d4cef7f785edb14f8055b3f52c13114))

## [1.18.0](https://github.com/wu/keyop-messenger/compare/v1.17.0...v1.18.0) (2026-06-25)

### Features

* **stats:** expose dropped/paused counters and oldest-pending latency ([d09c769](https://github.com/wu/keyop-messenger/commit/d09c76940e656fbdae8ad44e0f852a1e13b46469))

## [1.17.0](https://github.com/wu/keyop-messenger/compare/v1.16.0...v1.17.0) (2026-06-24)

### Features

* **federation:** add inbound hub-side metrics to Stats() ([7bea19f](https://github.com/wu/keyop-messenger/commit/7bea19fb89c3b93e60068ae5604ec5cd4c552e97))
* **federation:** declare client publish channels to the hub ([8440e40](https://github.com/wu/keyop-messenger/commit/8440e407d0b0dcbce5f58a99d2c9a33d50606bcc))

## [1.16.0](https://github.com/wu/keyop-messenger/compare/v1.15.1...v1.16.0) (2026-06-23)

### Features

* **federation:** batch hub ingest and unify the inbound commit path ([85d280e](https://github.com/wu/keyop-messenger/commit/85d280e4b081875261521dc719c5fefeb61fbd32))
* **federation:** batch receive-side commits with strict ack-after-commit ([59f9e4d](https://github.com/wu/keyop-messenger/commit/59f9e4dc41b5eb1d6f7504adfa16d10def413163))
* **messenger:** add PublishBatch built on WriteBatch ([aacf374](https://github.com/wu/keyop-messenger/commit/aacf374972e85b86fbc2202756125cd5e7bc97c8))
* **storage:** add WriteBatch for single-fsync batch appends ([cc0b18f](https://github.com/wu/keyop-messenger/commit/cc0b18f17d13587c6e431e0bb68b2ac0519319e3))

## [1.15.1](https://github.com/wu/keyop-messenger/compare/v1.15.0...v1.15.1) (2026-06-22)

### Bug Fixes

* skip records larger than maxLineSize instead of wedging the subscriber ([3a5d83f](https://github.com/wu/keyop-messenger/commit/3a5d83f30ed474b258af33c4f5af46552e4c0858))

## [1.15.0](https://github.com/wu/keyop-messenger/compare/v1.14.0...v1.15.0) (2026-06-22)

### Features

* add per-subscription startup max-age; remove max_subscriber_lag_mb ([6f5258a](https://github.com/wu/keyop-messenger/commit/6f5258aec2649e2e09cc33493feab050ac4a4ec9))
* bound channel disk usage with size and age retention ([f1d0866](https://github.com/wu/keyop-messenger/commit/f1d086676e0a4aedd79f9bbc6bfef4a521f8703e))

## [1.14.0](https://github.com/wu/keyop-messenger/compare/v1.13.1...v1.14.0) (2026-06-21)

### Features

* add ErrRetryLater for transient handler failures ([8ece79b](https://github.com/wu/keyop-messenger/commit/8ece79b1b519fdcf8dc82a08ac92736c9365cddc))

## [1.13.1](https://github.com/wu/keyop-messenger/compare/v1.13.0...v1.13.1) (2026-06-19)

### Bug Fixes

* **federation:** raise gRPC frame limit and skip undeliverable records ([4394443](https://github.com/wu/keyop-messenger/commit/4394443ceb4f85606a9ea808c8cbeae2455e0d3b))
* **messenger:** release audit writer when New fails after start ([224e077](https://github.com/wu/keyop-messenger/commit/224e077ce3ad59f749fc4cf5b7c0cb3f9a9594a8))

## [1.13.0](https://github.com/wu/keyop-messenger/compare/v1.12.0...v1.13.0) (2026-06-17)

### Features

* add diagnostic counters and stats for subscriber and notifier ([02d40d8](https://github.com/wu/keyop-messenger/commit/02d40d8634e0f0971735a6fd7196c0835e1aba7d))
* add periodic polling to prevent lost-wakeup races in subscriber ([21fa3a5](https://github.com/wu/keyop-messenger/commit/21fa3a5e80fb450f629d2f03d7d9324da5310ab5))
* enhance CI configuration for multi-architecture  tests ([8bc0682](https://github.com/wu/keyop-messenger/commit/8bc0682e8edb3a980fa481aa9117f25cb4e494ed))
* increase timeout for tests on raspi (armv6 and arm64) ([0af5d81](https://github.com/wu/keyop-messenger/commit/0af5d81f5997cd8cd3eb9a46aedabd4dbf6838ff))

### Bug Fixes

* do not emit partial trailing lines from subscriber scanner ([e996468](https://github.com/wu/keyop-messenger/commit/e9964682d942772d0306e745959876519e93a40b))
* truncate partial trailing bytes on writer startup ([d40902c](https://github.com/wu/keyop-messenger/commit/d40902c90efa681b3ecbe51bb98251b17677f857))

### Refactoring

* consolidate test steps and extend timeout for throughput tests ([a6ca2a9](https://github.com/wu/keyop-messenger/commit/a6ca2a9d1b8678b354d451f98c4c4778a5de3a37))

## [1.12.0](https://github.com/wu/keyop-messenger/compare/v1.11.1...v1.12.0) (2026-06-16)

### Features

* add 'inspect' command to keygen for detailed certificate information ([86f4e20](https://github.com/wu/keyop-messenger/commit/86f4e207ab4dc02721107a66a6e577e61f8f0f85))
* add Forgejo Actions workflow for private dev ([#1](https://github.com/wu/keyop-messenger/issues/1)) ([31fa405](https://github.com/wu/keyop-messenger/commit/31fa405d65461b524cee7ca9851e34def93304a4))

## [1.11.1](https://github.com/wu/keyop-messenger/compare/v1.11.0...v1.11.1) (2026-06-13)

### Bug Fixes

* **release:** renumber v2/v3/v4 changelog entries into v1 lineage ([48a5563](https://github.com/wu/keyop-messenger/commit/48a556340444737ef86b790658eaa6397ac6d210))

## [1.11.0](https://github.com/wu/keyop-messenger/compare/v1.10.0...v1.11.0) (2026-06-13)

### Features

* hardcode TLS 1.3 minimum; drop tls.min_version config field ([46b1cea](https://github.com/wu/keyop-messenger/commit/46b1cea0e94b7002a223874c2aa86cc919f7f606))
* tighten federation TLS verifier; mark BasicConstraints on leaves ([392582d](https://github.com/wu/keyop-messenger/commit/392582d706c75745538661cc88915d4ea30a8887))
* verify federation TLS by CA chain only, not by DNS-SAN matching ([75b46a7](https://github.com/wu/keyop-messenger/commit/75b46a7ed652afada1f2c26ddc3a85e9b40d6efb))

## [1.10.0](https://github.com/wu/keyop-messenger/compare/v1.9.0...v1.10.0) (2026-06-13)

### Features

* require TLS only when federation is enabled ([56a8073](https://github.com/wu/keyop-messenger/commit/56a8073b3bf4554ec92bd3f1a41efd25d6300077))

## [1.9.0](https://github.com/wu/keyop-messenger/compare/v1.8.1...v1.9.0) (2026-06-13)

### Features

* derive instance identity from TLS cert CN; unify peer auth ([a558284](https://github.com/wu/keyop-messenger/commit/a558284d1ed7ad95386973810338737b441b7eff))

## [1.8.1](https://github.com/wu/keyop-messenger/compare/v1.8.0...v1.8.1) (2026-06-13)

### Bug Fixes

* honor ctx cancellation in ChannelWriter.Write after hand-off ([5d042f6](https://github.com/wu/keyop-messenger/commit/5d042f660fa59ed26bb6721142c7b54a9c0a340f))
* wire subCtx to EphemeralClient Subscribe stream ([542415a](https://github.com/wu/keyop-messenger/commit/542415aad4144ea4b6135e69ae518f43ebeee78b))

## [1.8.0](https://github.com/wu/keyop-messenger/compare/v1.7.0...v1.8.0) (2026-06-13)

### ⚠ BREAKING CHANGES

* drive client-side federation publish from disk-pull readers

### Features

* drive client-side federation publish from disk-pull readers ([94ccd5c](https://github.com/wu/keyop-messenger/commit/94ccd5c0c9a305a1853784bde67d91a0750afa60))

## [1.7.0](https://github.com/wu/keyop-messenger/compare/v1.6.0...v1.7.0) (2026-06-13)

### ⚠ BREAKING CHANGES

* federated messaging protocol changed

### Features

* add workflow_dispatch to release.yaml for manual trigger support ([92b3599](https://github.com/wu/keyop-messenger/commit/92b35999575c80fb8edbf975c92d2faaca3e7eb1))
* bumping version for incompatible change ([0d1db16](https://github.com/wu/keyop-messenger/commit/0d1db1688a83ddfdbbb59a55ef5ddfcff96e5c79))

## [1.6.0](https://github.com/wu/keyop-messenger/compare/v1.5.0...v1.6.0) (2026-06-13)

### Features

* bumping version for incompatible change ([ae56819](https://github.com/wu/keyop-messenger/commit/ae568194dcf5c880101ccd4b3c17b2ebca247e42))

## [1.5.0](https://github.com/wu/keyop-messenger/compare/v1.4.1...v1.5.0) (2026-06-13)

### Features

* migrate federation layer from HTTP/1.1 WebSocket to gRPC ([e78ce9c](https://github.com/wu/keyop-messenger/commit/e78ce9c75f4f9ec9c4e6436048b403f1ea4f3d83))

### Bug Fixes

* drain PeerSender buf into unacked on stream close ([4c9c6c2](https://github.com/wu/keyop-messenger/commit/4c9c6c28c46abb9fd8e24dfaf2eff38cbed69f1f))
* eliminate Add/Wait data race on Hub.handlerWg ([7e9bc74](https://github.com/wu/keyop-messenger/commit/7e9bc7451a66185e50f305b60a6f53db76eace3a))

## [1.4.1](https://github.com/wu/keyop-messenger/compare/v1.4.0...v1.4.1) (2026-06-12)

### Bug Fixes

* count federated messages in Stats() MessageCount ([cefdfbc](https://github.com/wu/keyop-messenger/commit/cefdfbc0dcb955fa85eea6511efdc70456997464))
* poll for offset persistence in TestChannelReader_DeliveryAndOffsetPersistence ([e716d7a](https://github.com/wu/keyop-messenger/commit/e716d7a2411ef2ac0103d83324bfb751f63c8c4d))

## [1.4.0](https://github.com/wu/keyop-messenger/compare/v1.3.0...v1.4.0) (2026-06-12)

### Features

* add Stats() observability to Messenger ([6d26721](https://github.com/wu/keyop-messenger/commit/6d2672125d44ba88981840cc5a13c345d716194f))

## [1.3.0](https://github.com/wu/keyop-messenger/compare/v1.2.0...v1.3.0) (2026-05-17)

### Features

* update UUID generation to use version 7 in envelope ([9a080ee](https://github.com/wu/keyop-messenger/commit/9a080ee922e10ceb0d4981beceb589be422b7664))

## [1.2.0](https://github.com/wu/keyop-messenger/compare/v1.1.0...v1.2.0) (2026-05-13)

### Features

* enhance compaction logic to delete sealed segments when no local consumers are present ([a1d52a2](https://github.com/wu/keyop-messenger/commit/a1d52a272ba22e9cc85da94cccdad7235b5d0d70))

## [1.1.0](https://github.com/wu/keyop-messenger/compare/v1.0.0...v1.1.0) (2026-04-29)

### Features

* add release target to Makefile for pre-release checks ([649e0a0](https://github.com/wu/keyop-messenger/commit/649e0a0011496609c9b1c499775a34c8d293ef64))
* enhance example project for getting started guide ([349148b](https://github.com/wu/keyop-messenger/commit/349148b0edd62c06f561df9b4e2d5da73de20ae4))

### Bug Fixes

* ensure clients on publish to configured channels ([afff88f](https://github.com/wu/keyop-messenger/commit/afff88f36e535681086658dde071cad5629e74c8))

## [1.0.0](https://github.com/wu/keyop-messenger/compare/v0.14.1...v1.0.0) (2026-04-28)

### ⚠ BREAKING CHANGES

* removed SyncPolicy from config

### Features

* improve sync behavior ([ce59c02](https://github.com/wu/keyop-messenger/commit/ce59c0260c68d4dda5ac6bdb4ded542ef10b28da))

## [0.14.1](https://github.com/wu/keyop-messenger/compare/v0.14.0...v0.14.1) (2026-04-26)

### Refactoring

* clean up lint warnings ([7d24d5b](https://github.com/wu/keyop-messenger/commit/7d24d5bae2e0de85605f1d98d7a8814d05a7fc75))
* remove 'hot reload' section from docs ([3076528](https://github.com/wu/keyop-messenger/commit/307652846d14d7fed693912ad86e61fbc0b06940))

## [0.14.0](https://github.com/wu/keyop-messenger/compare/v0.13.0...v0.14.0) (2026-04-24)

### Features

* enhance ReadFrame to skip oversized records ([bf6df07](https://github.com/wu/keyop-messenger/commit/bf6df0782f47e7a5718a0e887d74c58daeb954e7))

## [0.13.0](https://github.com/wu/keyop-messenger/compare/v0.12.0...v0.13.0) (2026-04-24)

### Features

* update MaxBatchBytes default value to 4 MiB, log stop error ([11d4a15](https://github.com/wu/keyop-messenger/commit/11d4a15da2c20a711979269c336ea4b1a606f3d4))

## [0.12.0](https://github.com/wu/keyop-messenger/compare/v0.11.0...v0.12.0) (2026-04-24)

### Features

* enhance audit logging for client connection and disconnection events ([e773f72](https://github.com/wu/keyop-messenger/commit/e773f72c9f918948aa71fab2b16ea275862b30b8))

## [0.11.0](https://github.com/wu/keyop-messenger/compare/v0.10.1...v0.11.0) (2026-04-15)

### Features

* update federated clients to work more like local clients ([7d05a1a](https://github.com/wu/keyop-messenger/commit/7d05a1a0319a42ca6de3882c7ddaee6634f82223))

## [0.10.1](https://github.com/wu/keyop-messenger/compare/v0.10.0...v0.10.1) (2026-04-13)

### Bug Fixes

* resolve issue with publish channel support and client receive policy ([23a251f](https://github.com/wu/keyop-messenger/commit/23a251f2e3171dab59d2f1d7160cef8b5a89f893))

## [0.10.0](https://github.com/wu/keyop-messenger/compare/v0.9.1...v0.10.0) (2026-04-13)

### Features

* add ServiceName context field to track message publishers ([2d82aba](https://github.com/wu/keyop-messenger/commit/2d82abaf35126066616e0b81cf237fa4dd86a14a))

## [0.9.1](https://github.com/wu/keyop-messenger/compare/v0.9.0...v0.9.1) (2026-04-13)

### Bug Fixes

* resolve a bug with client channel filtering ([70791ba](https://github.com/wu/keyop-messenger/commit/70791ba885845664501bbd15f22ec9d4a50d8d17))

## [0.9.0](https://github.com/wu/keyop-messenger/compare/v0.8.0...v0.9.0) (2026-04-11)

### Features

* add support for correlation IDs for message tracing ([860b61f](https://github.com/wu/keyop-messenger/commit/860b61f9a69aa4468db4fd8d4ee2405167730f7e))

## [0.8.0](https://github.com/wu/keyop-messenger/compare/v0.7.0...v0.8.0) (2026-04-11)

### Features

* optimize peer message routing with reverse index and add cleanup tests ([acacb00](https://github.com/wu/keyop-messenger/commit/acacb00a0bcc310cfeae039d05684bdff0bd556c))
* remove hot-reload logic and related tests for allowlist policies ([f148423](https://github.com/wu/keyop-messenger/commit/f14842311351f90d93403ec4260df1b060b09cdb))

## [0.7.0](https://github.com/wu/keyop-messenger/compare/v0.6.2...v0.7.0) (2026-04-11)

### Features

* add client-to-client message forwarding test and improve hub forwarding logic ([7b2fad6](https://github.com/wu/keyop-messenger/commit/7b2fad6082bc6b0dffa8099b21c0bdacdc04bbb9))

### Bug Fixes

* add shared mutex to synchronize concurrent writes to WebSocket connections ([ab87f8f](https://github.com/wu/keyop-messenger/commit/ab87f8fc9f6c10a97559ab36ff3c7ca608f6741e))

## [0.6.2](https://github.com/wu/keyop-messenger/compare/v0.6.1...v0.6.2) (2026-04-10)

### Refactoring

* unify client and peer configurations under a single `allowed_peers` abstraction ([76dd0cf](https://github.com/wu/keyop-messenger/commit/76dd0cf51b7ff1f53d089d44dbd2e9eb4aeaecb8))

## [0.6.1](https://github.com/wu/keyop-messenger/compare/v0.6.0...v0.6.1) (2026-04-10)

### Bug Fixes

* return context.Canceled error if dial context is canceled ([770ea1d](https://github.com/wu/keyop-messenger/commit/770ea1d71439cd802f9ee65bd726219e6e6520c4))

## [v0.6.0](https://github.com/wu/keyop-messenger/compare/v0.5.0...v0.6.0) (2026-04-10)

### Chores

* update release tag format to include "v" prefix ([2047d3d](https://github.com/wu/keyop-messenger/commit/2047d3d0a9a1e15ba1dd1e09c69b44f9fcbb6e90))

## [v0.5.0](https://github.com/wu/keyop-messenger/compare/v0.4.0...v0.5.0) (2026-04-10)

### Features

* add InstanceName method and tests ([c025026](https://github.com/wu/keyop-messenger/commit/c0250269aa6efc25db593f4296406add485a0a0f))

## [v0.4.0](https://github.com/wu/keyop-messenger/compare/v0.3.0...v0.4.0) (2026-04-09)

### Features

* add Ephemeral Client support with live-only messaging and stateless operation ([9ae9c40](https://github.com/wu/keyop-messenger/commit/9ae9c40f2f38f32197f3735a9da71d3f2f7584d3))

### Refactoring

* clean up failing lint tests ([20f8ad6](https://github.com/wu/keyop-messenger/commit/20f8ad66c16a99d02f56c7bf0ff430823a3d76a1))

## [v0.3.0](https://github.com/wu/keyop-messenger/compare/v0.2.0...v0.3.0) (2026-04-09)

### Features

* add godoc badge, testing semantic release workflow ([c737dd2](https://github.com/wu/keyop-messenger/commit/c737dd2c2896e9587cb3ef99835e29645be4499c))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v1.7.0.html).

## [Unreleased]

## [v0.2.0] - 2026-04-09

### Added
- Federation support with mTLS WebSocket connections between instances
- Star topology hub-client architecture
- Per-client channel allowlists
- Audit logging for cross-hub forwarding events
- Policy hot-reloading for federation rules

### Changed
- Enhanced subscriber offset tracking for distributed scenarios

## [v0.1.0] - 2026-04-09

### Added
- Initial public release
- Append-only `.jsonl` segment files for durable message storage
- At-least-once delivery semantics
- Persistent offset tracking for subscribers
- Type-safe payload registry
- Configurable retry logic with dead-letter queue support
- Low-latency dual-layer notification system (LocalNotifier + fsnotify)
- File-based observability via standard Unix tools
- CLI tool for key generation (`keygen ca`, `keygen instance`)
