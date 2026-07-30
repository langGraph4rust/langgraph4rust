# langgraph4rust 🦀

[![CI](https://github.com/langGraph4rust/langgraph4rust/actions/workflows/ci.yml/badge.svg)](https://github.com/langGraph4rust/langgraph4rust/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)
[![Docs.rs](https://docs.rs/langgraph4rust/badge.svg)](https://docs.rs/langgraph4rust)
[![Downloads](https://img.shields.io/crates/d/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

**PythonのLangGraphライブラリにインスパイアされた、強力なRust製ステートフルワークフローエンジン。**

`langgraph4rust`は、柔軟で型安全かつ非同期優先のフレームワークを提供し、複雑なワークフローグラフの構築・実行・管理を可能にします。並列実行と条件付きルーティングをサポートしています。

## ✨ 特徴

- **🏗️ 宣言的グラフ構築**: 直感的なビルダーパターンでワークフローを定義
- **⚡ 並列実行**: 依存関係が許す場合、複数のノードを同時に実行可能
- **🔀 条件付きルーティング**: 実行時の状態条件に基づいて動的に経路を選択
- **💾 状態管理**: JSONベースの組み込み状態永続化と完全な型安全性
- **🔌 拡張可能なアーキテクチャ**: トレイトによるカスタムノード実装
- **✅ 包括的な検証**: 実行前のグラフ構造検証でランタイムエラーを防止
- **🎯 非同期優先設計**: Tokioベースで効率的な非同期操作を実現

## 📦 インストール

`Cargo.toml`に以下を追加：

```toml
[dependencies]
langgraph4rust = "0.1.1"
```

## 🚀 クイックスタート

### 基本例

```rust
use langgraph4rust::*;
use std::collections::HashSet;
use std::sync::Arc;
use async_trait::async_trait;

// カスタムノードを定義
#[derive(Clone)]
struct GreetingNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for GreetingNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        let message = "Hello from langgraph4rust! 🚀";
        println!("{}", message);
        state.set("greeting", message).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), LangGraphError> {
    // グラフビルダーを作成
    let mut builder = StateGraphBuilder::new();

    // ノードをグラフに追加
    builder.add_node("greet", Box::new(GreetingNode));

    // エッジを定義（ワークフロー接続）
    builder.add_edge(START_NODE, HashSet::from(["greet".to_string()]));
    builder.add_edge("greet", HashSet::from([END_NODE.to_string()]));

    // コンパイルして実行
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state).await?;
    
    Ok(())
}
```

この例を実行：
```bash
cargo run --example hello_world
```

## 🎯 コアコンセプト

### ノード 🔷

ノードはワークフローの基本構成要素です。各ノードは[`AgentNode`]トレイトを実装し、共有状態を処理・変更するロジックを含みます。

```rust
#[derive(Clone)]
struct MyNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for MyNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        // ノードのロジック
        state.set("result", "processed").await?;
        Ok(())
    }
}
```

### エッジ ➡️

エッジはノード間の制御フローを定義します：

- **静的エッジ**: 常に固定されたターゲットノードに接続
- **条件エッジ**: 現在の状態に基づいてターゲットを動的に選択

```rust
// 静的エッジ
builder.add_edge("node_a", HashSet::from(["node_b".to_string()]));

// 条件ルーティング（conditional_routing例を参照）
builder.add_conditional_edges(
    "decision_node",
    |state| async move { /* ルーティングロジック */ },
    HashMap::from([
        ("option_a".to_string(), HashSet::from(["node_x".to_string()])),
        ("option_b".to_string(), HashSet::from(["node_y".to_string()])),
    ])
)?;
```

### 状態 💾

状態は全ノード間で共有され、実行全体を通じて持続します：

```rust
let state = Arc::new(DefaultMemoryState::new());

// 値を設定
state.set("key", "value").await?;

// 型指定された値を取得
let value: String = state.get("key").await?.unwrap();
```

## 📚 例

`examples/`ディレクトリで完全な動作例を探索してください：

| 例 | 説明 |
|-----|------|
| [hello_world](examples/hello_world.rs) | シンプルなリニアワークフロー - 完璧な入門ポイント |
| [conditional_routing](examples/conditional_routing.rs) | 状態に基づく動的な経路選択 |
| [parallel_execution](examples/parallel_execution.rs) | 並行ノード実行 |
| [custom_state](examples/custom_state.rs) | カスタム状態バックエンドの実装 |
| [data_pipeline](examples/data_pipeline.rs) | マルチステージデータ処理パイプライン |
| [error_handling](examples/error_handling.rs) | 堅牢なエラー処理戦略 |

任意の例を実行：
```bash
cargo run --example <example_name>
```

## 🏗️ アーキテクチャ

```
┌─────────────────────────────────────────────┐
│              StateGraphBuilder              │
│  (宣言的グラフ構築API)                      │
└──────────────────┬──────────────────────────┘
                   │ compile()
                   ▼
┌─────────────────────────────────────────────┐
│               StateGraph                    │
│  (検証済みの実行可能ワークフロー)            │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐  │
│  │ Node A  │──▶│ Node B  │──▶│ Node C  │  │
│  └─────────┘   └─────────┘   └─────────┘  │
│         ▲                         │        │
│         └──────────(state)────────┘        │
└─────────────────────────────────────────────┘
                   │ invoke()
                   ▼
┌─────────────────────────────────────────────┐
│          DefaultMemoryState                 │
│  (JSONベースの永続的状態ストレージ)          │
└─────────────────────────────────────────────┘
```

## 🔧 API リファレンス

### コアタイプ

- **[`StateGraphBuilder`](src/core/state_graph_builder.rs)**: ワークフローグラフ構築用ビルダー
- **[`StateGraph`](src/core/state_graph.rs)**: コンパイル済み実行可能グラフインスタンス
- **[`AgentNode`](src/core/agent_node.rs)**: カスタムノード実装用トレイト
- **[`AgentState`](src/core/agent_state.rs)**: 状態管理バックエンド用トレイト
- **[`DefaultMemoryState`](src/core/agent_state.rs)**: 組み込みJSONベース状態実装
- **[`LangGraphError`](src/core/error.rs)**: ライブラリのエラー型

### 主要メソッド

```rust
// グラフ構築
StateGraphBuilder::new()                          // 新規ビルダー作成
builder.add_node(name, node)                      // ノード追加
builder.add_edge(from, to)                        // 静的エッジ追加
builder.add_conditional_edges(from, router, map)  // 条件エッジ追加
builder.compile()                                 // 検証＆グラフ構築

// ワークフロー実行
graph.invoke(initial_state)                       // ワークフロー実行

// 状態管理
state.set(key, value).await                       // 値保存
state.get::<T>(key).await?                        // 型指定値取得
```

## 🧪 テスト

テストスイートを実行：

```bash
# ユニットテスト
cargo test

# 特定の例テスト実行
cargo test --example hello_world

# 統合テスト（利用可能な場合）
cargo test --test integration_test
```

## 🤝 貢献

貢献をお待ちしています！お気軽にPull Requestを提出してください。

1. リポジトリをフォーク
2. 機能ブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. Pull Requestを開く

### 開発環境セットアップ

```bash
# リポジトリをクローン
git clone https://github.com/langGraph4rust/langgraph4rust.git
cd langgraph4rust

# プロジェクトをビルド
cargo build

# 例を実行
cargo run --example hello_world

# テストを実行
cargo test
```

## 📋 要件

- **Rust**: 2024エディション以降
- **Tokio**: 非同期ランタイム（依存関係に含まれています）
- **プラットフォーム**: macOS、Linux、Windows（テスト済み）

## 📄 ライセンス

このプロジェクトはApacheライセンス2.0の下でライセンスされています - 詳細は[LICENSE](LICENSE)ファイルをご覧ください。

## 🙏 謝辞

- [LangChain/LangGraph](https://github.com/langchain-ai/langgraph) Pythonライブラリにインスパイアされました
- [Rust](https://www.rust-lang.org/) エコシステムツールで構築
- 非同期ランタイムは[Tokio](https://tokio.rs/)により駆動

## 📞 サポート

- 📖 使用パターンについては[examples](examples/)をご覧ください
- 🐛 問題報告は[GitHub Issues](https://github.com/langGraph4rust/langgraph4rust/issues)から
- 💬 ディスカッションは[GitHub Discussions](https://github.com/langGraph4rust/langgraph4rust/discussions)へどうぞ

---

**Rustの型システムと非同期機能を使用して ❤️ で構築**
EOF 