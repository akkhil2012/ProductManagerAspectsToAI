# Data Platform Orchestration Pipeline

This folder stitches the three Spring Boot microservices in this repository into a single, repeatable data pipeline. The Python script orchestrates the following stages:

1. **Data Ingestion Service** – stores the raw events that arrive from external systems.
2. **Data Deduplication Service** – collapses duplicate events by their original `source_record_id`.
3. **Data Quality Service** – validates the curated dataset against domain rules before downstream consumption.

The pipeline uses the services through their REST APIs, which are exposed when the Spring Boot applications run locally (either directly with `mvn spring-boot:run` or through Docker Compose).

## Repository Layout

```
pipeline/
├── README.md                # This document
├── pipeline_config.yaml     # Base URLs and HTTP settings for each service
├── run_pipeline.py          # Python orchestrator
└── sample_data.json         # Example payload with duplicates and quality issues
```

## Quick Start

1. **Start the microservices** (in separate terminals):
   ```bash
   cd CompleteDataPipeline/data-platform-springboot-microservices
   docker-compose up --build
   ```
   or run each Spring Boot application with `mvn spring-boot:run`.

2. **Install the orchestration dependencies** (only required once):
   ```bash
   pip install requests pyyaml
   ```

3. **Execute the pipeline**:
   ```bash
   python pipeline/run_pipeline.py --log-level INFO
   ```
   The script will POST records to each service in sequence. Use the `--simulate` flag to dry-run the pipeline without making HTTP requests (useful when the services are not running).

   ```bash
   python pipeline/run_pipeline.py --simulate --log-level DEBUG
   ```

## How It Works

- **Configuration Driven:** `pipeline_config.yaml` describes where each microservice runs. Update the URLs if you expose the services on different hosts or ports.
- **Stage Summaries:** Each stage collects the payload sent to the service and the returned response, which makes it easy to debug the pipeline or feed the results into monitoring dashboards.
- **Embedded Data Rules:** The orchestrator applies basic business rules in the quality stage (positive purchase amounts and valid email formats). You can extend `_evaluate_quality` with domain-specific checks.
- **Sample Dataset:** `sample_data.json` includes duplicates and a purposely invalid record so that the deduplication and quality stages produce meaningful output.

## Extending the Pipeline

- Add new services by editing `pipeline_config.yaml` and creating another `_run_<stage>` method in `run_pipeline.py`.
- Replace the inline validation logic with calls to the microservices' `/process` or `/validate` endpoints if you implement additional Python-based workflows.
- Persist or broadcast the stage results by serialising `StageResult` objects to disk, publishing them to Kafka, or triggering downstream analytics jobs.

## Troubleshooting

| Symptom | Possible Cause | Suggested Fix |
|--------|----------------|---------------|
| `PipelineError` about configuration | Missing or misnamed service key | Check `pipeline_config.yaml` for typos |
| Connection refused errors | Services are not running | Start the Spring Boot apps or run in `--simulate` mode |
| Non-200 HTTP responses | Validation failure in a service | Inspect the `response` payload printed at the end of the run |

Happy pipelining! 🚀
