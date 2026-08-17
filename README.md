# BLE Prometheus Collector

SwitchBotのBLE広告から温度・湿度を取得し、Prometheus形式で公開するコレクターです。

## ディレクトリ構成

```text
.
├── deploy/
│   ├── docker/               # Dockerイメージとローカル監視環境、その設定
│   └── kubernetes/           # Kubernetesマニフェスト
├── mise.toml                 # ツールバージョンと開発タスク
├── src/ble_prometheus_collector/
│   ├── collector.py          # BLEスキャンとメトリクス公開
│   ├── parsers.py            # BLEペイロードの解析
│   └── settings.py           # 環境変数の読み込みと検証
└── tests/                    # 単体テスト
```

ルート直下にはプロジェクト全体のメタデータと案内だけを置き、実装・設定・デプロイ資材を責務ごとに分離しています。ペイロード解析をI/O処理から切り離したため、Bluetooth機器なしでもテストできます。

## セットアップと実行

```bash
mise install
mise run setup
mise run collector
```

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>（初期ユーザー `admin` / パスワード `admin123`）
- Collector metrics: <http://localhost:8000/metrics>

## 対象機器の設定

対象機器は環境変数 `TARGET_MAC_ADDRESSES` にカンマ区切りで指定します。そのうちLegacy形式の機器を `LEGACY_MAC_ADDRESSES` に指定し、それ以外には標準パーサーを使用します。値は起動時に大文字へ正規化され、不正なMACアドレスや対象外のLegacy機器が含まれている場合は起動に失敗します。

`TARGET_MAC_ADDRESSES`の未指定時は従来の2台、`LEGACY_MAC_ADDRESSES`の未指定時は従来のLegacy機器が対象に含まれている場合だけLegacyとして扱われます。Legacy機器がない場合は空文字を指定できます。

```bash
TARGET_MAC_ADDRESSES="AA:BB:CC:DD:EE:FF,11:22:33:44:55:66" \
LEGACY_MAC_ADDRESSES="AA:BB:CC:DD:EE:FF" \
mise run collector
```

Dockerの場合:

```bash
docker run --network host \
  -e TARGET_MAC_ADDRESSES="AA:BB:CC:DD:EE:FF,11:22:33:44:55:66" \
  -e LEGACY_MAC_ADDRESSES="AA:BB:CC:DD:EE:FF" \
  ble-prometheus-collector
```

Kubernetesでは `deploy/kubernetes/overlays/production/collector.env` の両方の環境変数を変更して注入します。

## Kubernetesへのデプロイ

KubernetesマニフェストはKustomizeのbase/overlay構成です。Bluetoothアダプターを持つノードの準備、設定変更、差分確認、デプロイ手順は `deploy/kubernetes/README.md` を参照してください。

```bash
mise run k8s:crd:install
mise run k8s:build
mise run k8s:diff
mise run k8s:deploy
```

## 開発時の確認

```bash
mise run test
mise run lint
mise run compose:check
# または全項目をまとめて実行
mise run check
```

利用できるタスクは `mise tasks` で確認できます。監視環境の停止には `mise run monitoring:down` を使います。

Dockerイメージをビルドする場合は、リポジトリルートをコンテキストにします。

```bash
mise run image:build:arm64
```

タグを変更する場合やGHCRへpushする場合:

```bash
BLE_IMAGE=ghcr.io/example/ble-collector:v1 mise run image:build:arm64
BLE_IMAGE=ghcr.io/example/ble-collector:v1 mise run image:push:arm64
```
