# ReID Service (Meta Edge Server)

![alt text](assets/Flow_diagram_reid_11.03.26.drawio.png)

This service runs on the **meta edge server (not Jetson)** and is responsible for:

* Receiving extracted vehicle embeddings (from FE container on the Jetson)
* one message -> a "sighting" (saved in MinIO datalake)
* Aggregating single-track sightings into "vehicle events"
* Each vehicle event gets a feature vector centroid
* Storing and querying feature vector centroids (OpenSearch k-NN)
* ReIdentified vehicle events get placed under stame vehicle ID
* Long term storage of data in MinIO Data lake

---

# Input / Output Overview

## Input (Sighting Stream)

The service consumes a stream of **sightings**


## MQTT Input Format

When using `INPUT_MODE=mqtt`, the service expects messages in this format:

```json id="mqtt_in_1"
{
    "cam_id": "cam_1",
    "track_id": 42,
    "timestamp_ns": 1747728320000000000,
    "bbox": [523, 214, 702, 412],
    "image": "<hex_encoded_png>",
    "features": [0.12, -0.44, ...]
}
```

### Fields

| Field          | Type        | Description                         |
| -------------- | ----------- | ----------------------------------- |
| `cam_id`       | string      | Camera identifier                   |
| `track_id`     | int         | Local tracking ID (per camera)      |
| `timestamp_ns` | int         | Capture timestamp (ns)              |
| `bbox`         | list[int]   | Bounding box                        |
| `image`        | string      | Encoded crop image (hex → PNG/JPEG) |
| `features`     | list[float] | ReID embedding vector               |

---

## Output (Persistent Storage + Indexing)

Each sighting produces **two parallel outputs**:

### 1. MinIO Datalake Storage

Sightings stored immediately:

```
sightings/{YYYY/MM/DD/uuid}.json
images/{...}.png
sightings_embeddings/{model_name}/{...}.npy
```

Also later aggregated into:

```
vehicle_events/{event_id}.json
event_embeddings/{model_name}/{...}.npy
```

---

### 2. OpenSearch Vector Index

Each vehicle event embeddings centroid is inserted into OpenSearch:

```json id="os_doc"
{
    "object_key": "YYYY/MM/DD/uuid",
    "vehicle_id": "uuid or assigned id",
    "camera_id": "cam_1",
    "track_id": 42,
    "feature_vector": [ ... ],
    "timestamp_ns": 1747728320000000000
}
```

This enables:

* Cross-camera similarity search
* k-NN retrieval of similar vehicles
* Automatic cleanup of old vectors
* Cleanup, that only affects OpenSearch vectors
* MinIO data remaining permanent

---

# Vehicle Re-Identification Logic

When a track is finalized:

1. All embeddings are normalized
2. A centroid vector is computed
3. Cross-camera search is executed in OpenSearch

### Matching process:

* Retrieve top-K similar vectors (excluding same camera)
* Group results by `vehicle_id`
* Aggregate scores:

  * max score
  * mean score
  * support count bonus
* Apply threshold filtering
* Apply ambiguity margin filtering
* Optionally override with high-confidence match

---

# Runtime Configuration



| Variable                      | Default  | Description                                                  |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| `TRACK_TIMEOUT_SECONDS`       | `30`     | Finalize a track if no new sightings arrive within this time |
| `REID_THRESHOLD`              | `0.77`   | Minimum aggregated similarity score required for a match     |
| `REID_OVERRULE_THRESHOLD`     | `0.92`   | Accept best match even if multiple candidates remain         |
| `VECTOR_RETENTION_MINUTES`    | `5` | Delete vectors older than this age from OpenSearch  (hot index)         |
| `DB_CLEANUP_INTERVAL_SECONDS` | `30`     | How often search for expired vectors is performed in Opensearch |

## Track Finalization

All sightings from the same:

    (camera_id, track_id)

are grouped into a single vehicle event.

A track is finalized when:

    last_seen_runtime_ns + (TRACK_TIMEOUT_SECONDS * 1e9) expires

After finalization:

* sighting embeddings are aggregated into a centroid that is indexed into Opensearch
* event and individual sightings data is stored in MinIO fileserver

## ReID Matching

After a track is finalized, the service:

1. Searches OpenSearch for similar vehicles from other cameras
2. Groups similar results by existing vehicle_id
3. Chooses the best matching vehicle if similarity scores pass configured thresholds
4. Creates a new vehicle_id if no reliable match is found

If multiple vehicles have very similar scores, the match may be rejected unless the best score is high enough to pass the overrule threshold.

## Vector Cleanup

Old vectors can be deleted automatically to reduce OpenSearch storage size and ReID accuracy.

If `VECTOR_RETENTION_MINUTES` is set:

    timestamp_ns < current_time_ns - retention_window

vectors are periodically removed from OpenSearch.

Example:

    -e VECTOR_RETENTION_MINUTES=5

removes vectors older than 5 minutes.

If unset, vector cleanup is disabled.

## Index reset

By default the vector index is deleted after closing the container and a new vector index is always created when launching the container.

This can be changed with:

```sh
-e RESET_INDEX=false

```

# Launch Requirements

## Required environment

The service requires:

* OpenSearch cluster access
* MinIO object storage access
* MQTT/Kafka input stream (or JSONL file)
* Shared Docker network for OpenSearch
* Mounted TLS certificates (for MQTT)

---

## Docker Launch Command

(launch in the same folder where the certificates are found)

Input mode is controlled via:

- `INPUT_MODE=mqtt` → single Jetson (direct broker connection)
- `INPUT_MODE=kafka` → multi-Jetson streaming (broker aggregation)
- `INPUT_MODE=json` → offline/testing mode

ReID service input source is interchangeable and does not affect downstream processing logic.

### Implementation with MQTT broker
With MQTT mode, the service connects directly to **a single Jetson broker** instance.

#### Minimal/default launch

```bash id="docker_run"
docker run --rm -d \
  --network opensearch_opensearch-net \
  -v $(pwd):/certs \
  -e OS_HOST=opensearch-node1 \
  -e OS_PORT=9200 \
  -e OS_USER=edi \
  -e OS_PASSWORD=*** \
  -e MINIO_ENDPOINT=d42edgeai:9090 \
  -e MINIO_ACCESS_KEY=reid-test \
  -e MINIO_SECRET_KEY=*** \
  -e MINIO_BUCKET=reid-test \
  -e INPUT_MODE=mqtt \
  -e MQTT_HOST=edgejet4vpn.edi.lv \
  -e MQTT_PORT=8884 \
  -e MQTT_TOPIC=reid-vehicle-analysis \
  -e MQTT_CA_CERT=/certs/ca-cert \
  -e MQTT_CERT=/certs/client.crt \
  -e MQTT_KEY=/certs/client.key \
  -e VECTOR_RETENTION_MINUTES=5 \
  -e TRACK_TIMEOUT_SECONDS=210 \
  ghcr.io/tomasszu/reidservice:demo
```

#### Tuned/custom launch

```bash id="docker_run"
docker run --rm -d \
  --network opensearch_opensearch-net \
  -v $(pwd):/certs \
  -e OS_HOST=opensearch-node1 \
  -e OS_PORT=9200 \
  -e OS_USER=edi \
  -e OS_PASSWORD=*** \
  -e MINIO_ENDPOINT=d42edgeai:9090 \
  -e MINIO_ACCESS_KEY=reid-test \
  -e MINIO_SECRET_KEY=*** \
  -e MINIO_BUCKET=reid-test \
  -e INPUT_MODE=mqtt \
  -e MQTT_HOST=edgejet4vpn.edi.lv \
  -e MQTT_PORT=8884 \
  -e MQTT_TOPIC=reid-vehicle-analysis \
  -e MQTT_CA_CERT=/certs/ca-cert \
  -e MQTT_CERT=/certs/client.crt \
  -e MQTT_KEY=/certs/client.key \
  -e TRACK_TIMEOUT_SECONDS=210 \
  -e REID_THRESHOLD=0.77 \
  -e REID_OVERRULE_THRESHOLD=0.92 \
  -e DB_CLEANUP_INTERVAL_SECONDS=60 \
  -e VECTOR_RETENTION_MINUTES=2 \
  -e RESET_INDEX=false \
  ghcr.io/tomasszu/reidservice:demo
```

### Implementation with Kafka bridging

Kafka mode aggregates messages from **all Jetsons** through a central broker.

> ⚠️ Kafka mode is currently implemented in the service but depends on partner-side Kafka setup. It may not be operational until external infrastructure is fully enabled.

Kafka mode requires access to TLS certificates located in the meta-edge certificate directory.

#### Minimal/default launch

```sh
docker run --rm -d \
  --network opensearch_opensearch-net \
  --add-host dfb.tech:10.0.0.5 \
  -v $(pwd):/certs \
  -e OS_HOST=opensearch-node1 \
  -e OS_PORT=9200 \
  -e OS_USER=edi \
  -e OS_PASSWORD=*** \
  -e MINIO_ENDPOINT=d42edgeai:9090 \
  -e MINIO_ACCESS_KEY=reid-test \
  -e MINIO_SECRET_KEY=*** \
  -e MINIO_BUCKET=reid-test \
  -e INPUT_MODE=kafka \
  -e KAFKA_BOOTSTRAP=dfb.tech:9093 \
  -e KAFKA_TOPIC=reid-vehicle-analysis \
  -e KAFKA_CA_CERT=/certs/trusted_authority.cert \
  -e KAFKA_CERT=/certs/signed.pem \
  -e KAFKA_KEY=/certs/signed.key \
  -e KAFKA_GROUP_ID=edgeai-vcd42-edi \
  -e VECTOR_RETENTION_MINUTES=5 \
  -e TRACK_TIMEOUT_SECONDS=210 \
  ghcr.io/tomasszu/reidservice:demo

```



---

## Certificate Assumption

The container expects certificates in the mounted folder.

e.g. in `/home/eduser/tomass/edgeai-vcd42-edi-edgejet4vpn/client-certs` for MQTT coming from Jetson #4

or `/home/eduser/tomass/edgeai-vcd42-edi-metaedge/certificates-created` for Kafka

For MQTT:

```text id="certs"
./ca-cert
./client.crt
./client.key
```

For KAFKA:

```text id="certs"
./trusted_authority.cert
./signed.key
./signed.pem
```

Mounted into:

```
/certs
```

---


# Shutdown Behavior

On SIGTERM / SIGINT:

* Stop ingestion loop
* Stop maintenance thread
* Optionally delete OpenSearch index (`RESET_INDEX=true`)
* Flush pending state

---

# Key Design Notes

* TrackManager temporarily stores sightings in memory until a track is finalized.
* OpenSearch hot-index vector search (entries expire after time)
* MinIO (raw data lake) - all data saved here (this is the part you see in data app).
* ReID decision is delayed until track closure (event-based aggregation)
* Cross-camera matching is centroid-based, not per-frame
