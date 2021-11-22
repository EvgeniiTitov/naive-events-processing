import time
import keyboard

from app import ElasticApp


def main() -> int:
    app = ElasticApp()
    app.start()

    while True:
        pressed_key = keyboard.read_key()
        if pressed_key == "q":
            break
        elif pressed_key == "u":
            app.increase_load()
        elif pressed_key == "d":
            app.decrease_load()
        time.sleep(0.2)

    app.stop()
    return 0


if __name__ == "__main__":
    main()
