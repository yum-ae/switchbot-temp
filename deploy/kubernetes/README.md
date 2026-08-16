# Kubernetes deployment

## 前提条件

- ARM64のKubernetesノードにBluetoothアダプターとBlueZがあること
- ホストに `/run/dbus`、`/sys/class/bluetooth`、`/dev` があること
- Prometheus Operatorと `PodMonitor` CRDが導入されていること
- 現在のkubectl contextがデプロイ対象クラスターを指していること

`PodMonitor` CRDがない場合は、miseからPrometheus Operator v0.93.0の公式CRDを導入します。この操作にはクラスター単位のCRD作成権限が必要です。

```bash
mise run k8s:crd:install
mise run k8s:crd:status
```

CRDだけではメトリクス収集は開始されません。別途Prometheus Operatorが稼働し、このPodMonitorを選択する設定になっている必要があります。

## 設定

本番設定は `overlays/production/collector.env` で変更します。ConfigMap名には内容のハッシュが付き、設定変更時にはPodが自動的に再作成されます。

```dotenv
TARGET_MAC_ADDRESSES=D4:35:34:35:68:4D,F2:B2:02:06:7C:49
LEGACY_MAC_ADDRESSES=D4:35:34:35:68:4D
```

イメージタグは `overlays/production/kustomization.yaml` の `images` で管理します。本番運用では `latest` 系ではなく、リリースタグまたはdigestへの固定を推奨します。

ARM64イメージのローカルビルドとレジストリへのpushはmiseから実行できます。デフォルトのタグはDeploymentと同じ `ghcr.io/yum-ae/ble-collector:latest-arm64` です。

```bash
mise run image:build:arm64
mise run image:push:arm64
```

## 検証とデプロイ

```bash
mise run k8s:crd:install
mise run k8s:build
mise run k8s:diff
mise run k8s:deploy
```

リソースは専用の `ble-monitoring` namespaceに作成されます。DeploymentはBLEアダプターの同時利用を避けるため `Recreate` 戦略とし、readiness/liveness probe、リソース上限、capability制限、seccomp、専用ServiceAccountを設定しています。
