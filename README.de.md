# langgraph4rust 🦀

[![CI](https://github.com/langGraph4rust/langgraph4rust/actions/workflows/ci.yml/badge.svg)](https://github.com/langGraph4rust/langgraph4rust/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)
[![Docs.rs](https://docs.rs/langgraph4rust/badge.svg)](https://docs.rs/langgraph4rust)
[![Downloads](https://img.shields.io/crates/d/langgraph4rust.svg)](https://crates.io/crates/langgraph4rust)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE
[![Rust](https://img.shields.io/badge/Rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

**Eine leistungsstarke Rust-Implementierung einer zustandsbehafteten Workflow-Engine, inspiriert von Pythons LangGraph-Bibliothek.**

`langgraph4rust` bietet ein flexibles, typsicheres und asynchrones Framework zum Erstellen, Ausführen und Verwalten komplexer Workflow-Grafen mit Unterstützung für parallele Ausführung und bedingtes Routing.

## ✨ Funktionen

- **🏗️ Deklarativer Grafen-Aufbau**: Definition von Workflows mit einem intuitiven Builder-Muster
- **⚡ Parallele Ausführung**: Mehrere Knoten können gleichzeitig ausgeführt werden, wenn Abhängigkeiten dies zulassen
- **🔀 Bedingtes Routing**: Dynamische Pfadauswahl basierend auf Laufzeitzustandsbedingungen
- **💾 Zustandsverwaltung**: Integrierte JSON-basierte Zustandspersistenz mit vollständiger Typsicherheit
- **🔌 Erweiterbare Architektur**: Benutzerdefinierte Knotenimplementierungen über Traits
- **✅ Umfassende Validierung**: Grafenstrukturvalidierung vor der Ausführung verhindert Laufzeitfehler
- **🎯 Async-First-Design**: Auf Tokio aufbauend für effiziente asynchrone Operationen

## 📦 Installation

Fügen Sie dies zu Ihrem `Cargo.toml` hinzu:

```toml
[dependencies]
langgraph4rust = "0.1.1"
```

## 🚀 Schnellstart

### Grundlegendes Beispiel

```rust
use langgraph4rust::*;
use std::collections::HashSet;
use std::sync::Arc;
use async_trait::async_trait;

// Definieren Sie einen benutzerdefinierten Knoten
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
    // Grafen-Builder erstellen
    let mut builder = StateGraphBuilder::new();

    // Knoten zum Grafen hinzufügen
    builder.add_node("greet", Box::new(GreetingNode));

    // Kanten definieren (Workflow-Verbindungen)
    builder.add_edge(START_NODE, HashSet::from(["greet".to_string()]));
    builder.add_edge("greet", HashSet::from([END_NODE.to_string()]));

    // Kompilieren und ausführen
    let graph = builder.compile()?;
    let state = Arc::new(DefaultMemoryState::new());
    
    graph.invoke(state).await?;
    
    Ok(())
}
```

Führen Sie dieses Beispiel aus:
```bash
cargo run --example hello_world
```

## 🎯 Kernkonzepte

### Knoten 🔷

Knoten sind die grundlegenden Bausteine Ihres Workflows. Jeder Knoten implementiert das [`AgentNode`]-Trait und enthält Logik zur Verarbeitung und Änderung des gemeinsamen Zustands.

```rust
#[derive(Clone)]
struct MyNode;

#[async_trait]
impl AgentNode<DefaultMemoryState> for MyNode {
    async fn apply(&self, state: Arc<DefaultMemoryState>) -> Result<(), LangGraphError> {
        // Ihre Knoten-Logik hier
        state.set("result", "processed").await?;
        Ok(())
    }
}
```

### Kanten ➡️

Kanten definieren den Kontrollfluss zwischen Knoten:

- **Statische Kanten**: Verbinden immer mit festen Zielknoten
- **Bedingte Kanten**: Wählen dynamisch Ziele basierend auf dem aktuellen Zustand

```rust
// Statische Kante
builder.add_edge("node_a", HashSet::from(["node_b".to_string()]));

// Bedingtes Routing (siehe conditional_routing Beispiel)
builder.add_conditional_edges(
    "decision_node",
    |state| async move { /* Routing-Logik */ },
    HashMap::from([
        ("option_a".to_string(), HashSet::from(["node_x".to_string()])),
        ("option_b".to_string(), HashSet::from(["node_y".to_string()])),
    ])
)?;
```

### Zustand 💾

Der Zustand wird über alle Knoten hinweg geteilt und während der gesamten Ausführung beibehalten:

```rust
let state = Arc::new(DefaultMemoryState::new());

// Werte setzen
state.set("key", "value").await?;

// Typisierte Werte abrufen
let value: String = state.get("key").await?.unwrap();
```

## 📚 Beispiele

Erkunden Sie das `examples/`-Verzeichnis für vollständige funktionierende Beispiele:

| Beispiel | Beschreibung |
|-----------|--------------|
| [hello_world](examples/hello_world.rs) | Einfacher linearer Workflow - perfekter Einstiegspunkt |
| [conditional_routing](examples/conditional_routing.rs) | Dynamische Pfadauswahl basierend auf Zustand |
| [parallel_execution](examples/parallel_execution.rs | Gleichzeitige Knotenausführung |
| [custom_state](examples/custom_state.rs) | Implementierung benutzerdefinierter Zustands-Backends |
| [data_pipeline](examples/data_pipeline.rs) | Mehrstufige Datenverarbeitungspipeline |
| [error_handling](examples/error_handling.rs) | Robuste Fehlerbehandlungsstrategien |

Beliebiges Beispiel ausführen:
```bash
cargo run --example <example_name>
```

## 🏗️ Architektur

```
┌─────────────────────────────────────────────┐
│              StateGraphBuilder              │
│  (Deklarative Grafen-Konstruktion API)      │
└──────────────────┬──────────────────────────┘
                   │ compile()
                   ▼
┌─────────────────────────────────────────────┐
│               StateGraph                    │
│  (Validierter, ausführbarer Workflow)       │
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
│  (JSON-basierte persistente Zustandsspeicherung) │
└─────────────────────────────────────────────┘
```

## 🔧 API-Referenz

### Kern-Typen

- **[`StateGraphBuilder`](src/core/state_graph_builder.rs)**: Builder zum Konstruieren von Workflow-Grafen
- **[`StateGraph`](src/core/state_graph.rs)**: Kompilierte, ausführbare Grafen-Instanz
- **[`AgentNode`](src/core/agent_node.rs)**: Trait zur Implementierung benutzerdefinierter Knoten
- **[`AgentState`](src/core/agent_state.rs)**: Trait für Zustandsverwaltungs-Backends
- **[`DefaultMemoryState`](src/core/agent_state.rs)**: Integrierte JSON-basierte Zustandsimplementierung
- **[`LangGraphError`](src/core/error.rs)**: Fehlertyp für die Bibliothek

### Wichtige Methoden

```rust
// Grafen aufbauen
StateGraphBuilder::new()                          // Neuen Builder erstellen
builder.add_node(name, node)                      // Knoten hinzufügen
builder.add_edge(from, to)                        // Statische Kante hinzufügen
builder.add_conditional_edges(from, router, map)  // Bedingte Kante hinzufügen
builder.compile()                                 // Validieren & Grafen bauen

// Workflows ausführen
graph.invoke(initial_state)                       // Workflow ausführen

// Zustand verwalten
state.set(key, value).await                       // Wert speichern
state.get::<T>(key).await?                        // Typisierten Wert abrufen
```

## 🧪 Testen

Führen Sie die Testsuite aus:

```bash
// Unit-Tests
cargo test

// Spezifische Beispieltests ausführen
cargo test --example hello_world

// Integrationstests (falls verfügbar)
cargo test --test integration_test
```

## 🤝 Beitrag

Beiträge sind willkommen! Fühlen Sie sich frei, Pull Requests einzureichen.

1. Forken Sie das Repository
2. Erstellen Sie Ihren Feature-Branch (`git checkout -b feature/amazing-feature`)
3. Committen Sie Ihre Änderungen (`git commit -m 'Add amazing feature'`)
4. Pushen Sie zum Branch (`git push origin feature/amazing-feature`)
5. Öffnen Sie einen Pull Request

### Entwicklungsumgebungseinrichtung

```bash
// Repository klonen
git clone https://github.com/langGraph4rust/langgraph4rust.git
cd langgraph4rust

// Projekt bauen
cargo build

// Beispiele ausführen
cargo run --example hello_world

// Tests ausführen
cargo test
```

## 📋 Anforderungen

- **Rust**: Edition 2024 oder neuer
- **Tokio**: Asynchronous Runtime (als Abhängigkeit enthalten)
- **Plattform**: macOS, Linux, Windows (getestet)

## 📄 Lizenz

Dieses Projekt steht unter der Apache Lizenz 2.0 - Details finden Sie in der [LICENSE](LICENSE)-Datei.

## 🙏 Danksagungen

- Inspiriert von der [LangChain/LangGraph](https://github.com/langchain-ai/langgraph) Python-Bibliothek
- Gebaut mit Werkzeugen des [Rust](https://www.rust-lang.org/) Ökosystems
- Asynchronous Runtime powered by [Tokio](https://tokio.rs/)

## 📞 Support

- 📖 Schauen Sie sich die [examples](examples/) für Verwendungsmuster an
- 🐛 Melden Sie Probleme über [GitHub Issues](https://github.com/langGraph4rust/langgraph4rust/issues)
- 💬 Diskussionen willkommen in [GitHub Discussions](https://github.com/langGraph4rust/langgraph4rust/discussions)

---

**Gebuilt mit ❤️ unter Verwendung von Rusts Typensystem und asynchronen Fähigkeiten**
EOF 