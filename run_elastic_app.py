import cv2

from app import ElasticApp


def main() -> int:
    app = ElasticApp()
    app.start()

    while True:
        pressed_key = cv2.waitKey(33)
        if pressed_key == 27:
            break
        elif pressed_key == ord("u"):
            app.increase_load()
        elif pressed_key == ord("d"):
            app.decrease_load()

    app.stop()
    return 0


if __name__ == "__main__":
    main()
