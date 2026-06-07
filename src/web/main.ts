import { AnsiUp } from "./ansi_up";

const ansi_up = new AnsiUp();
ansi_up.escapeForHtml = true;

const current_path = window.location.pathname;
const ws_protocol = window.location.protocol === "https:" ? "wss:" : "ws:";

const ws_url = `${ws_protocol}//${window.location.host}${current_path}/ws`;
const socket = new WebSocket(ws_url);

let ACTIVE_BUILD = false;

const load_build_button = document.getElementById(
    "load-build-button",
)! as HTMLButtonElement;
load_build_button.addEventListener("click", () => {
    if (!ACTIVE_BUILD) {
        window.location.href = "/";
    }
});

const log_container = document.getElementById(
    "log-container",
)! as HTMLDivElement;
const scrollToBottom = () => {
    log_container.scrollTop = log_container.scrollHeight;
};

let ERROR_OCCURED = false;

interface SocketInit {
    ty: "Init";
    is_active: boolean;
}

interface SocketMessage {
    ty: "Message" | "Error";
    payload: string;
    stage: number;
}

type SocketData = SocketMessage | SocketInit;

socket.onmessage = (event) => {
    const message: SocketData = JSON.parse(event.data);
    console.log(message);

    switch (message.ty) {
        case "Init": {
            ACTIVE_BUILD = message.is_active;
            if (ACTIVE_BUILD)
                load_build_button.textContent = "Build in progress...";
            break;
        }
        case "Message": {
            const logs_elem = document.getElementById(
                `log-messages-${message.stage}`,
            )! as HTMLPreElement;

            const html_payload = ansi_up.ansi_to_html(message.payload);
            logs_elem.innerHTML += "\n" + html_payload;

            scrollToBottom();
            break;
        }
        case "Error": {
            if (message.payload !== "") {
                ERROR_OCCURED = true;

                const error_elem = document.getElementById(
                    `log-error-${message.stage}`,
                )! as HTMLPreElement;
                error_elem.textContent = message.payload;

                scrollToBottom();
            }
            break;
        }
    }
};

socket.onclose = (_) => {
    if (ACTIVE_BUILD) {
        const button_message = ERROR_OCCURED
            ? "Build failed. Return to previous homepage..."
            : "Build succeeded! Go to homepage...";
        load_build_button.textContent = button_message;

        ACTIVE_BUILD = false;
    }

    scrollToBottom();
};
