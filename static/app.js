"use strict";

const $ = (id) => document.getElementById(id);

const EXAMPLES = {
  assign: `# 赋值示例
x = 1
y = 2
total = x + y
name = "alice"
msg = name + " has " + str(total) + " apples"
print(msg)`,

  func: `# 函数调用示例
def add(a, b):
    result = a + b
    return result

def main():
    x = add(3, 4)
    y = add(x, 10)
    print("y =", y)

main()`,

  loop: `# 循环示例
total = 0
for i in range(5):
    total += i
    print("i =", i, "total =", total)

n = 3
while n > 0:
    n -= 1
print("done", total)`,

  exc: `# 异常示例
def div(a, b):
    return a / b

values = [10, 5, 0]
for v in values:
    print("100 /", v, "=", div(100, v))
print("never reached")`,
};

const STATUS_TEXT = {
  idle: "空闲",
  running: "运行中",
  paused: "已暂停",
  finished: "已结束",
  exception: "异常终止",
  terminated: "已终止",
};

let runId = null;      // run id of the run this page is driving
let pauseSeq = 0;      // pause sequence number of the latest pause
let pollTimer = null;

function showMessage(text) {
  $("message").textContent = text || "";
}

function updateLineCount() {
  const lines = $("code").value.split("\n").length;
  const el = $("line-count");
  el.textContent = `${lines} / ${MAX_LINES} 行`;
  el.classList.toggle("over", lines > MAX_LINES);
}

function setRunningUI(running) {
  $("code").readOnly = running;               // 运行期间源码只读
  $("examples").disabled = running;
  $("btn-run").disabled = running;
  $("btn-terminate").disabled = !running;
  if (running) {
    $("btn-step").disabled = true;
    $("btn-continue").disabled = true;
  }
}

function startPolling() {
  stopPolling();
  pollTimer = setInterval(poll, 400);
  poll();
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, data };
}

async function startRun() {
  showMessage("");
  const { ok, data } = await postJSON("/api/run", { code: $("code").value });
  if (!ok) {
    showMessage(data.error || "启动失败");
    return;
  }
  runId = data.run_id;
  pauseSeq = 0;
  setRunningUI(true);
  startPolling();
}

async function control(kind) {
  if (!runId) return;
  // 同一次停顿只发一次；服务端也会拒绝重复/过期请求
  $("btn-step").disabled = true;
  $("btn-continue").disabled = true;
  const { ok, data } = await postJSON(`/api/${kind}`, {
    run_id: runId,
    pause_seq: pauseSeq,
  });
  if (!ok) {
    showMessage(data.error || "请求被拒绝");
  } else {
    showMessage("");
  }
  setTimeout(poll, 60);
}

async function terminateRun() {
  if (!runId) return;
  const { ok, data } = await postJSON("/api/terminate", { run_id: runId });
  if (!ok) showMessage(data.error || "终止失败");
  poll();
}

async function poll() {
  if (!runId) return;
  let data;
  try {
    const res = await fetch("/api/state");
    data = await res.json();
  } catch (e) {
    return;
  }
  // 旧运行的迟到响应不得覆盖新运行：run_id 不匹配直接丢弃
  if (data.run_id !== runId) return;
  render(data);
  if (["finished", "exception", "terminated"].includes(data.status)) {
    stopPolling();
    setRunningUI(false);
  }
}

function render(state) {
  const badge = $("status");
  badge.textContent = STATUS_TEXT[state.status] || state.status;
  badge.className = "badge badge-" + state.status;

  pauseSeq = state.pause_seq;
  const paused = state.status === "paused";
  $("btn-step").disabled = !paused;
  $("btn-continue").disabled = !paused;

  renderListing(state.lines, state.current_line, state.status);
  renderLocals(state.locals);

  let out = state.stdout || "";
  if (state.stdout_truncated) out = "（输出过长，仅保留末尾部分）\n" + out;
  $("stdout").textContent = out;
  $("exception").textContent = state.exception || "";
}

function renderListing(lines, currentLine, status) {
  const listing = $("listing");
  listing.textContent = "";
  listing.classList.remove("empty");
  lines.forEach((text, i) => {
    const row = document.createElement("div");
    row.className = "code-line";
    if (i + 1 === currentLine && status !== "finished") {
      row.classList.add("current");
    }
    const gutter = document.createElement("span");
    gutter.className = "gutter";
    gutter.textContent = (i + 1 === currentLine && status !== "finished" ? "→ " : "  ")
      + String(i + 1).padStart(2, " ");
    const code = document.createElement("span");
    code.className = "text";
    code.textContent = text || " ";
    row.appendChild(gutter);
    row.appendChild(code);
    listing.appendChild(row);
  });
}

function renderLocals(locals) {
  const tbody = $("locals").querySelector("tbody");
  tbody.textContent = "";
  const names = Object.keys(locals || {});
  if (names.length === 0) {
    const tr = document.createElement("tr");
    tr.className = "empty";
    const td = document.createElement("td");
    td.textContent = "（无）";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  for (const name of names) {
    const tr = document.createElement("tr");
    const tdName = document.createElement("td");
    tdName.className = "var-name";
    tdName.textContent = name;
    const tdVal = document.createElement("td");
    tdVal.className = "var-value";
    tdVal.textContent = locals[name];
    tr.appendChild(tdName);
    tr.appendChild(tdVal);
    tbody.appendChild(tr);
  }
}

$("btn-run").addEventListener("click", startRun);
$("btn-step").addEventListener("click", () => control("step"));
$("btn-continue").addEventListener("click", () => control("continue"));
$("btn-terminate").addEventListener("click", terminateRun);
$("code").addEventListener("input", updateLineCount);
$("examples").addEventListener("change", (e) => {
  const code = EXAMPLES[e.target.value];
  if (code) {
    $("code").value = code + "\n";
    updateLineCount();
  }
  e.target.value = "";
});

updateLineCount();
