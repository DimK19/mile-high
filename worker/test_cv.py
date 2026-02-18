import cv2
import time
import json
import uuid
import datetime
from ultralytics import YOLO

# ---------------------------------------------------------
# 1. SETUP & CONFIGURATION
# ---------------------------------------------------------
MOCK_CHUNK_ID = "chunk_123"
MOCK_VIDEO_ID = "cam_01_highway"
MOCK_CHUNK_INDEX = 1

# --- YOUR SPEED GATE CONFIG ---
LINE_A = 282   # Top line
LINE_B = 370   # Bottom line
GATE_DISTANCE_METERS = 17.0

# Define a relative path to your video
VIDEO_SOURCE = "C:\\Users\\KYRIAKOS\\Desktop\\segments\\segment17.mp4"

def utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

print("Loading YOLOv8 model...")
model = YOLO('yolov8s.pt').to('cuda')

cap = cv2.VideoCapture(VIDEO_SOURCE)
fps = cap.get(cv2.CAP_PROP_FPS) or 30.0

# ---------------------------------------------------------
# 2. STATE MANAGEMENT
# ---------------------------------------------------------
# Stores the last known Y-position to detect line crossing
# { track_id: {"y": float, "timestamp": float} }
track_last_pos = {}

# Stores the time a vehicle crossed specific lines
# { track_id: { "t_417": float, "t_553": float } }
gate_crossings = {}

# Stores final results to print at the end
track_summaries = {}

print(f"--- PLAYING VIDEO: {VIDEO_SOURCE} ---")
print(f"--- SPEED GATE ACTIVE: Y={LINE_A} to Y={LINE_B} ({GATE_DISTANCE_METERS}m) ---")
print("Press 'q' to stop early and see the JSON report.")

while cap.isOpened():
    success, frame = cap.read()
    if not success:
        break

    # Current timestamp in the video (in seconds)
    current_time = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0

    # Draw the Speed Gate Lines
    cv2.line(frame, (0, LINE_A), (frame.shape[1], LINE_A), (0, 255, 255), 2) # Yellow Line A
    cv2.line(frame, (0, LINE_B), (frame.shape[1], LINE_B), (0, 255, 255), 2) # Yellow Line B

    # Run YOLO Tracking
    results = model.track(frame, persist=True, verbose=False)

    if results[0].boxes.id is not None:
        boxes = results[0].boxes.xywh.cpu().tolist()
        track_ids = results[0].boxes.id.int().cpu().tolist()
        cls_indices = results[0].boxes.cls.int().cpu().tolist()

        for box, track_id, cls_idx in zip(boxes, track_ids, cls_indices):
            class_name = model.names[cls_idx]

            # Filter: Only vehicles
            if class_name not in ['car', 'truck', 'bus', 'motorcycle']:
                continue

            x, y, w, h = box

            # -----------------------------------------------------
            # 3. SPEED GATE LOGIC (Crossing Detection)
            # -----------------------------------------------------
            # We need previous position to know if it *just* crossed
            if track_id in track_last_pos:
                prev_y = track_last_pos[track_id]["y"]
                prev_t = track_last_pos[track_id]["timestamp"]

                # Initialize crossing dict for this car if needed
                if track_id not in gate_crossings:
                    gate_crossings[track_id] = {}

                # Check crossing LINE A (417)
                # Did it move from above to below? (Down/Incoming) OR below to above? (Up/Outgoing)
                if (prev_y < LINE_A <= y) or (y < LINE_A <= prev_y):
                    gate_crossings[track_id]["t_A"] = current_time

                # Check crossing LINE B (553)
                if (prev_y < LINE_B <= y) or (y < LINE_B <= prev_y):
                    gate_crossings[track_id]["t_B"] = current_time

                # --- CALCULATE SPEED IF BOTH LINES CROSSED ---
                if "t_A" in gate_crossings[track_id] and "t_B" in gate_crossings[track_id]:
                    # Determine time difference
                    t1 = gate_crossings[track_id]["t_A"]
                    t2 = gate_crossings[track_id]["t_B"]
                    duration = abs(t2 - t1)

                    if duration > 0.1: # Avoid noise (instantly hitting both?)
                        speed_mps = GATE_DISTANCE_METERS / duration
                        speed_kmh = speed_mps * 3.6

                        # Determine Direction based on which line was hit first
                        direction = "inbound" if t1 < t2 else "outbound"
                        # (Assuming Top->Bottom is Inbound. Swap if needed)

                        # Store/Update Summary
                        track_summaries[track_id] = {
                            "track_id": f"{MOCK_CHUNK_ID}_{track_id}",
                            "vehicle_type": class_name,
                            "direction": direction,
                            "avg_speed_kmh": round(speed_kmh, 2),
                            "max_speed_kmh": round(speed_kmh, 2) # Constant speed for gate method
                        }

                        # Visual Feedback
                        label = f"{int(speed_kmh)} km/h"
                        cv2.rectangle(frame, (int(x-w/2), int(y-h/2)), (int(x+w/2), int(y+h/2)), (0, 255, 0), 2)
                        cv2.putText(frame, label, (int(x-w/2), int(y-h/2)-10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

                        # Clear crossing times so we don't recalculate continuously
                        # (Unless you want to update it? Usually once is enough)
                        del gate_crossings[track_id]

            # Update history
            track_last_pos[track_id] = {"y": y, "timestamp": current_time}

    cv2.imshow("Speed Gate Test", frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

cap.release()
cv2.destroyAllWindows()

# ---------------------------------------------------------
# 4. FINAL REPORT (JSON Output)
# ---------------------------------------------------------
print("\n" + "="*50)
print(" VIDEO FINISHED - GENERATING WORKER OUTPUT ")
print("="*50)

final_payloads = []
for t_id, data in track_summaries.items():
    kafka_message = {
        "event_type": "vehicle_track_summary",
        "event_id": str(uuid.uuid4()),
        "timestamp_utc": utc_now_iso(),
        "chunk_id": MOCK_CHUNK_ID,
        "source_video_id": MOCK_VIDEO_ID,
        "chunk_index": MOCK_CHUNK_INDEX,
        "track_id": data["track_id"],
        "vehicle_type": data["vehicle_type"],
        "direction": data["direction"],
        "avg_speed_kmh": data["avg_speed_kmh"],
        "max_speed_kmh": data["max_speed_kmh"],
    }
    final_payloads.append(kafka_message)

print(json.dumps(final_payloads, indent=2))
print("="*50)
print(f"Total Vehicles Measured: {len(final_payloads)}")
