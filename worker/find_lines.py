import cv2

# PATH TO YOUR VIDEO
VIDEO_SOURCE = "splitter/out/chunk_001.mp4" 

def draw_lines(event, x, y, flags, param):
    if event == cv2.EVENT_MOUSEMOVE:
        # Copy frame to draw on
        img_copy = frame.copy()
        # Draw a horizontal line where the mouse is
        cv2.line(img_copy, (0, y), (img_copy.shape[1], y), (0, 255, 0), 2)
        cv2.putText(img_copy, f"Y={y}", (10, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
        cv2.imshow('Find Lines', img_copy)

cap = cv2.VideoCapture(VIDEO_SOURCE)
ret, frame = cap.read()

if ret:
    cv2.imshow('Find Lines', frame)
    cv2.setMouseCallback('Find Lines', draw_lines)
    
    print("Hover over the road to find Y coordinates.")
    print("Press any key to close.")
    cv2.waitKey(0)

cap.release()
cv2.destroyAllWindows()