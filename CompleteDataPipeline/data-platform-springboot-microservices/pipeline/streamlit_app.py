"""Streamlit UI to visualize the Spring Boot microservices pipeline."""
from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, Tuple

import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

from run_pipeline import (
    PipelineRunner,
    load_config,
    load_records,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_PATH,
)


STAGES: Tuple[Dict[str, str], ...] = (
    {"key": "ingestion", "label": "Data Ingestion", "method": "_run_ingestion"},
    {
        "key": "deduplication",
        "label": "Data Deduplication",
        "method": "_run_deduplication",
        "dependency": "ingestion",
    },
    {
        "key": "quality",
        "label": "Data Quality",
        "method": "_run_quality",
        "dependency": "deduplication",
    },
    {
        "key": "normalization",
        "label": "Data Normalization",
        "method": "_run_normalization",
        "dependency": "quality",
    },
    {
        "key": "storage",
        "label": "Data Storage",
        "method": "_run_storage",
        "dependency": "normalization",
    },
    {
        "key": "consumption",
        "label": "Data Consumption",
        "method": "_run_consumption",
        "dependency": "storage",
    },
)

STAGE_METHOD_ARGS = {
    "_run_ingestion": tuple(),
    "_run_deduplication": ("ingestion",),
    "_run_quality": ("deduplication",),
    "_run_normalization": ("quality",),
    "_run_storage": ("normalization",),
    "_run_consumption": ("storage",),
}

STATUS_COLORS = {
    "pending": "#bdc3c7",
    "running": "#f39c12",
    "completed": "#27ae60",
}


def _stage_pairs() -> Iterable[Tuple[str, str]]:
    for index in range(len(STAGES) - 1):
        yield STAGES[index]["key"], STAGES[index + 1]["key"]


def _progress_to_color(progress: float) -> str:
    """Map a progress value in [0, 1] to a green gradient color."""
    progress = max(0.0, min(1.0, progress))
    # Transition from soft grey to green
    start_rgb = (189, 195, 199)
    end_rgb = (39, 174, 96)
    red = int(start_rgb[0] + (end_rgb[0] - start_rgb[0]) * progress)
    green = int(start_rgb[1] + (end_rgb[1] - start_rgb[1]) * progress)
    blue = int(start_rgb[2] + (end_rgb[2] - start_rgb[2]) * progress)
    return f"#{red:02x}{green:02x}{blue:02x}"


def render_pipeline_graph(statuses: Dict[str, str], edge_progress: Dict[str, float]) -> str:
    """Create a PyVis network graph for the pipeline and return HTML content."""
    net = Network(height="520px", width="100%", directed=True)
    net.barnes_hut()

    for stage in STAGES:
        key = stage["key"]
        net.add_node(
            key,
            label=stage["label"],
            color=STATUS_COLORS.get(statuses.get(key, "pending"), STATUS_COLORS["pending"]),
            shape="box",
            title=f"Status: {statuses.get(key, 'pending').title()}",
        )

    for source, target in _stage_pairs():
        progress = edge_progress.get(source, 0.0)
        net.add_edge(
            source,
            target,
            color=_progress_to_color(progress),
            width=2 + (progress * 4),
            smooth=True,
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.write_html(tmp_file.name, notebook=False)
        html = Path(tmp_file.name).read_text(encoding="utf-8")

    Path(tmp_file.name).unlink(missing_ok=True)

    return html


def _display_stage_records(stage_key: str, result) -> None:
    if not result.records:
        st.info(f"{stage_key.title()} produced no records")
        return

    with st.expander(f"{stage_key.title()} Output Records ({len(result.records)})", expanded=False):
        st.json(result.records)


@st.cache_data(show_spinner=False)
def load_default_config(path: Path) -> Dict:
    return load_config(path)


@st.cache_data(show_spinner=False)
def load_default_records(path: Path):
    return load_records(path)


st.set_page_config(page_title="Pipeline Orchestrator", layout="wide")
st.title("Data Platform Pipeline Visualizer")
st.markdown(
    """
    This Streamlit app visualizes the Spring Boot microservices pipeline. Each service is a node
    in the network graph and the connecting edges animate to show progress as the pipeline stages
    execute.
    """
)

sidebar = st.sidebar
sidebar.header("Configuration")
config_path = sidebar.text_input("Pipeline config path", value=str(DEFAULT_CONFIG_PATH))
records_path = sidebar.text_input("Sample data path", value=str(DEFAULT_DATA_PATH))
simulate_calls = sidebar.checkbox(
    "Simulate HTTP calls (recommended)",
    value=True,
    help="When enabled the orchestrator will not attempt to reach the real services.",
)

run_button = st.button("Run Pipeline")

status_placeholder = st.empty()
graph_container = st.container()
results_placeholder = st.container()
log_placeholder = st.empty()


if run_button:
    try:
        config = load_default_config(Path(config_path))
        raw_records = load_default_records(Path(records_path))
    except FileNotFoundError as exc:
        st.error(f"Failed to load configuration or records: {exc}")
    else:
        statuses = {stage["key"]: "pending" for stage in STAGES}
        edge_progress = {stage["key"]: 0.0 for stage in STAGES}
        results = {}

        graph_html = render_pipeline_graph(statuses, edge_progress)
        with graph_container:
            components.html(graph_html, height=540, scrolling=True)

        runner = PipelineRunner(config=config, raw_records=raw_records, simulate=simulate_calls)
        previous_results = {}

        for stage in STAGES:
            key = stage["key"]
            method_name = stage["method"]
            dependencies = STAGE_METHOD_ARGS.get(method_name, tuple())
            args = [previous_results[dep] for dep in dependencies]

            statuses[key] = "running"
            status_placeholder.markdown(f"**Running {stage['label']}...**")
            graph_html = render_pipeline_graph(statuses, edge_progress)
            with graph_container:
                components.html(graph_html, height=540, scrolling=True)

            method = getattr(runner, method_name)
            result = method(*args)  # type: ignore[misc]
            results[key] = result
            previous_results[key] = result

            total_records = max(1, len(result.records))
            for index in range(total_records):
                edge_progress[key] = (index + 1) / total_records
                graph_html = render_pipeline_graph(statuses, edge_progress)
                with graph_container:
                    components.html(graph_html, height=540, scrolling=True)
                time.sleep(0.2)

            statuses[key] = "completed"
            status_placeholder.success(f"Completed {stage['label']}")
            graph_html = render_pipeline_graph(statuses, edge_progress)
            with graph_container:
                components.html(graph_html, height=540, scrolling=True)

        status_placeholder.success("Pipeline execution finished")

        with results_placeholder:
            st.subheader("Stage Outputs")
            for stage in STAGES:
                _display_stage_records(stage["key"], results[stage["key"]])

        log_placeholder.info(
            "Simulation complete. If you need to target live services, disable simulation from the sidebar."
        )
else:
    statuses = {stage["key"]: "pending" for stage in STAGES}
    edge_progress = {stage["key"]: 0.0 for stage in STAGES}
    graph_html = render_pipeline_graph(statuses, edge_progress)
    with graph_container:
        components.html(graph_html, height=540, scrolling=True)
    status_placeholder.info("Awaiting pipeline execution")
