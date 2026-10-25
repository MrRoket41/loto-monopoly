/** Multiplayer room tests for the lotto + Monopoly game, through real WebSockets. */
import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";

async function open(room: string) {
  const res = await SELF.fetch(`https://game.test/ws/${room}`, { headers: { Upgrade: "websocket" } });
  expect(res.status).toBe(101);
  const ws = res.webSocket;
  if (!ws) throw new Error("no webSocket on upgrade response");
  ws.accept();
  const frames: any[] = [];
  ws.addEventListener("message", (event: MessageEvent) => {
    if (typeof event.data !== "string" || event.data === "__pong") return;
    try { frames.push(JSON.parse(event.data)); } catch { /* ignore malformed frames */ }
  });
  const next = async (pred: (f: any) => boolean, label: string) => {
    for (let i = 0; i < 100; i++) {
      const hit = frames.find(pred);
      if (hit) return hit;
      await scheduler.wait(10);
    }
    throw new Error(`timed out waiting for ${label}: ${JSON.stringify(frames)}`);
  };
  return { ws, frames, next, send: (m: unknown) => ws.send(JSON.stringify(m)), state: () => next(f => f?.type === "state", "state"), error: () => next(f => f?.type === "error", "error") };
}
let seq = 0;
const roomName = () => `loto-${++seq}-${crypto.randomUUID().slice(0, 6)}`;
async function joinThree(room: string) {
  const judge = await open(room), p1 = await open(room), p2 = await open(room);
  judge.send({ type: "join", playerId: "judge" });
  p1.send({ type: "join", playerId: "p1" });
  p2.send({ type: "join", playerId: "p2" });
  const [j, a, b] = await Promise.all([
    judge.next(f => f?.type === "state" && f.status === "playing", "room start for judge"),
    p1.next(f => f?.type === "state" && f.status === "playing", "room start for p1"),
    p2.next(f => f?.type === "state" && f.status === "playing", "room start for p2"),
  ]);
  return { judge, p1, p2, j, a, b };
}

describe("multiplayer lotto-monopoly", () => {
  it("waits for a separate judge and two players", async () => {
    const room = roomName();
    const judge = await open(room), p1 = await open(room), p2 = await open(room);
    judge.send({ type: "join", playerId: "judge" });
    expect((await judge.state()).status).toBe("waiting");
    p1.send({ type: "join", playerId: "p1" });
    expect((await p1.state()).status).toBe("waiting");
    p2.send({ type: "join", playerId: "p2" });
    const playing = await p2.next(f => f?.type === "state" && f.status === "playing", "three seats to start");
    expect(playing.meta.game).toBe("Лото-монополія");
    expect(playing.seats).toEqual(["judge", "p1", "p2"]);
    expect(playing.view.players).toHaveLength(2);
    expect(playing.view.isJudge).toBe(false);
    judge.ws.close(); p1.ws.close(); p2.ws.close();
  });

  it("synchronizes card choice and calculates the lotto payout", async () => {
    const { judge, p1, p2 } = await joinThree(roomName());
    p1.send({ type: "action", action: { type: "selectCards", player: 0, cards: [0, 1, 2] } });
    await p1.next(f => f?.type === "state" && f.view.players?.[0]?.cards === 3, "three selected cards");
    judge.send({ type: "action", action: { type: "configure", player: 0, cards: 3, correct: 23 } });
    await judge.next(f => f?.type === "state" && f.view?.players?.[0]?.correct === 23, "p1 score");
    judge.send({ type: "action", action: { type: "configure", player: 1, cards: 1, correct: 12 } });
    await judge.next(f => f?.type === "state" && f.view?.players?.[1]?.correct === 12, "p2 score");
    judge.send({ type: "action", action: { type: "difficulty", value: "hard" } });
    await judge.next(f => f?.type === "state" && f.view?.difficulty === "hard", "difficulty");
    judge.send({ type: "action", action: { type: "startMonopoly" } });
    const result = await p1.next(f => f?.type === "state" && f.view?.phase === "monopoly", "monopoly phase");
    expect(result.view.players[0].money).toBe(322);
    expect(result.view.players[1].money).toBe(504);
    judge.ws.close(); p1.ws.close(); p2.ws.close();
  });

  it("lets the judge roll, control money and keep all devices in sync", async () => {
    const { judge, p1, p2 } = await joinThree(roomName());
    judge.send({ type: "action", action: { type: "configure", player: 0, cards: 1, correct: 10 } });
    await judge.next(f => f?.type === "state" && f.view?.players?.[0]?.correct === 10, "p1 score");
    judge.send({ type: "action", action: { type: "configure", player: 1, cards: 1, correct: 12 } });
    await judge.next(f => f?.type === "state" && f.view?.players?.[1]?.correct === 12, "p2 score");
    judge.send({ type: "action", action: { type: "startMonopoly" } });
    await p1.next(f => f?.type === "state" && f.view?.phase === "monopoly", "monopoly start");
    judge.send({ type: "action", action: { type: "roll", player: 0 } });
    const rolled = await p2.next(f => f?.type === "state" && Number.isInteger(f.view?.dice), "dice result");
    expect(rolled.view.dice).toBeGreaterThanOrEqual(1);
    expect(rolled.view.dice).toBeLessThanOrEqual(6);
    const expectedMoney = rolled.view.players[0].money + 50;
    judge.send({ type: "action", action: { type: "money", player: 0, amount: 50 } });
    const adjusted = await p1.next(f => f?.type === "state" && f.view?.players?.[0]?.money === expectedMoney, "money update");
    expect(adjusted.view.players[0].money).toBe(expectedMoney);
    p2.send({ type: "action", action: { type: "money", player: 0, amount: 999 } });
    expect((await p2.error()).error).toContain("суддя");
    judge.ws.close(); p1.ws.close(); p2.ws.close();
  });

  it("adds late joiners without resetting the current room", async () => {
    const room = roomName();
    const a = await open(room), b = await open(room), c = await open(room), d = await open(room);
    a.send({ type: "join", playerId: "judge" });b.send({ type: "join", playerId: "p1" });c.send({ type: "join", playerId: "p2" });
    await c.next(f => f?.type === "state" && f.status === "playing", "initial game");
    d.send({ type: "join", playerId: "p3" });
    const expanded = await d.next(f => f?.type === "state" && f.view?.players?.length === 3, "late player seat");
    expect(expanded.view.phase).toBe("lotto");
    a.ws.close();b.ws.close();c.ws.close();d.ws.close();
  });
});
