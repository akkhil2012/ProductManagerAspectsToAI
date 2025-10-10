"""Streamlit UI to visualize data platform microservices pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import streamlit as st
from pyvis.network import Network


@dataclass(frozen=True)
class ServiceNode:
    """Representation of a microservice within the data pipeline."""

    identifier: str
    label: str
    description: str


PIPELINE_STEPS: List[ServiceNode] = [
    ServiceNode(
        identifier="dataingestion-service",
        label="Data Ingestion",
        description=(
            "Collects data from product analytics sources and APIs, preparing it for "
            "downstream processing."
        ),
    ),
    ServiceNode(
        identifier="datadeduplication-service",
        label="Data Deduplication",
        description=(
            "Removes duplicate records to ensure unique, high-quality data assets."
        ),
    ),
    ServiceNode(
        identifier="dataquality-service",
        label="Data Quality",
        description=(
            "Validates schema, completeness, and accuracy rules before persistence."
        ),
    ),
    ServiceNode(
        identifier="DataLineageStage",
        label="Data Lineage",
        description=(
            "Captures lineage metadata so teams can trace transformations across the pipeline."
        ),
    ),
]

PIPELINE_EDGES = [
    (PIPELINE_STEPS[index].identifier, PIPELINE_STEPS[index + 1].identifier)
    for index in range(len(PIPELINE_STEPS) - 1)
]


def build_network() -> Network:
    """Create a PyVis network with the pipeline topology."""

    network = Network(
        height="600px",
        width="100%",
        bgcolor="#0f172a",
        font_color="white",
        directed=True,
    )
    network.barnes_hut()

    for node in PIPELINE_STEPS:
        network.add_node(
            node.identifier,
            label=node.label,
            title=f"{node.label}: {node.description}",
            color="#38bdf8",
        )

    for source, target in PIPELINE_EDGES:
        network.add_edge(source, target, color="#facc15", arrowStrikethrough=False)

    network.set_options(
        """
        var options = {
          "nodes": {
            "font": {"size": 18},
            "shape": "dot",
            "scaling": {"min": 15, "max": 40}
          },
          "edges": {
            "color": {"color": "#facc15"},
            "smooth": false,
            "arrows": {
              "to": {"enabled": true, "scaleFactor": 1.2}
            }
          },
          "physics": {
            "barnesHut": {
              "gravitationalConstant": -8000,
              "springLength": 200,
              "springConstant": 0.04,
              "damping": 0.09
            },
            "minVelocity": 0.75
          }
        }
        """
    )

    return network


def main() -> None:
    st.set_page_config(page_title="Data Platform Pipeline", layout="wide")
    st.title("Data Platform Microservices Pipeline")
    st.markdown(
        """
        This topology view illustrates how the data platform microservices interact within
        the product insights pipeline. Each node represents a microservice, and edges indicate
        the order in which data flows through the pipeline.
        """
    )

    network = build_network()
    graph_html = network.generate_html(notebook=False)

    st.components.v1.html(graph_html, height=620, scrolling=True)

    st.subheader("Pipeline Stage Details")
    for step_number, node in enumerate(PIPELINE_STEPS, start=1):
        with st.expander(f"{step_number}. {node.label}"):
            st.write(node.description)


if __name__ == "__main__":
    main()
