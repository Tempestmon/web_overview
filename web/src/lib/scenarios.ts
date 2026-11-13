import type { FaultKind, NodeState, Snapshot, Topology } from './api';

export interface Step {
  /** What the visitor should see on the scheme. */
  text: string;
  /** Why the system behaves this way. */
  why: string;
  seen: (now: Snapshot, baseline: Snapshot) => boolean;
}

export interface Scenario {
  id: string;
  question: string;
  node: string;
  kind: FaultKind;
  /** Metric-driven effects need more than the default minute to show up. */
  ttlS: number;
  steps: Step[];
  lesson: string;
}

// Same share that paints an edge red on the scheme.
const VISIBLE_ERROR_SHARE = 0.05;
// Mirrors the controller's rule for a slow request/response edge.
const SLOW_P95_MS = 1000;
const LAG_GROWTH = 4;
const LAG_DRAINED = 3;

function stateOf(s: Snapshot, node: string): NodeState | undefined {
  return s.nodes.find((n) => n.id === node)?.state;
}

function edgeOf(s: Snapshot, from: string, to: string) {
  return s.edges.find((e) => e.from === from && e.to === to);
}

function hasTraffic(s: Snapshot, from: string, to: string): boolean {
  return (edgeOf(s, from, to)?.rps ?? 0) > 0.1;
}

function healed(s: Snapshot, node: string): boolean {
  return !s.faults.some((f) => f.node === node) && stateOf(s, node) === 'healthy';
}

const all = (s: Snapshot, nodes: string[], state: NodeState) => nodes.every((n) => stateOf(s, n) === state);
const breaker = (s: Snapshot) => edgeOf(s, 'order-svc', 'payment-svc')?.breaker;
const gateways = ['gateway-1', 'gateway-2'];

// The users → traefik edge is built from Traefik's own request counter and
// carries no error rate, so customer-facing errors are read one hop further,
// where the gateways report what they answered.
function customersSeeErrors(s: Snapshot): boolean {
  const answered = gateways.map((g) => edgeOf(s, 'traefik', g)).filter((e) => e !== undefined);
  const rps = answered.reduce((sum, e) => sum + e.rps, 0);
  const errors = answered.reduce((sum, e) => sum + e.error_rps, 0);
  return rps > 0 && errors / rps > VISIBLE_ERROR_SHARE;
}

export const SCENARIOS: Scenario[] = [
  {
    id: 'kitchen-stop',
    question: 'Что будет, если остановить кухню?',
    node: 'kitchen-svc',
    kind: 'stop',
    // Customers wait for their order before placing the next one, so the
    // backlog builds slowly and needs more than a minute to stand out.
    ttlS: 120,
    steps: [
      {
        text: 'Кухня стала красной',
        why: 'Контейнер остановлен, сообщения из Kafka никто не читает.',
        seen: (s) => stateOf(s, 'kitchen-svc') === 'broken',
      },
      {
        text: 'Заказы принимаются как раньше',
        why: 'order-svc кладёт заказ в топик и не ждёт кухню. Ему нужен брокер, а не потребитель.',
        seen: (s) => stateOf(s, 'order-svc') === 'healthy' && hasTraffic(s, 'users', 'traefik') && !customersSeeErrors(s),
      },
      {
        text: 'Очередь кухни растёт',
        why: 'Оплаченные заказы копятся в топике orders.created. Это видно в шапке и в Grafana.',
        seen: (s, base) => s.kafka_lag > base.kafka_lag + LAG_GROWTH,
      },
      {
        text: 'Кухня вернулась',
        why: 'Потребитель продолжает с последнего закоммиченного offset, ничего не потеряно.',
        seen: (s) => healed(s, 'kitchen-svc'),
      },
      {
        text: 'Очередь разобрана',
        why: 'Накопленные заказы доготовлены, просто позже.',
        seen: (s, base) => s.kafka_lag <= base.kafka_lag + LAG_DRAINED,
      },
    ],
    lesson: 'Очередь развязывает сервисы во времени: падение потребителя превращается в задержку, а не в ошибку.',
  },
  {
    id: 'gateway-stop',
    question: 'Что будет, если упадёт одна реплика gateway?',
    node: 'gateway-1',
    kind: 'stop',
    ttlS: 60,
    steps: [
      {
        text: 'gateway-1 красный',
        why: 'Реплика остановлена и не проходит health-check балансера.',
        seen: (s) => stateOf(s, 'gateway-1') === 'broken',
      },
      {
        text: 'Весь трафик идёт через gateway-2',
        why: 'Traefik убрал мёртвую реплику из ротации после проваленных проверок.',
        seen: (s) => !hasTraffic(s, 'traefik', 'gateway-1') && hasTraffic(s, 'traefik', 'gateway-2'),
      },
      {
        text: 'Покупатели ошибок не видят',
        why: 'Реплики не хранят состояния, поэтому любая из них обслужит любой запрос.',
        seen: (s) => hasTraffic(s, 'users', 'traefik') && !customersSeeErrors(s),
      },
      {
        text: 'gateway-1 снова в ротации',
        why: 'Health-check позеленел, балансер вернул реплику сам.',
        seen: (s) => healed(s, 'gateway-1') && hasTraffic(s, 'traefik', 'gateway-1'),
      },
    ],
    lesson: 'Реплики без состояния за балансером с health-check переживают потерю одной из них без участия человека.',
  },
  {
    id: 'postgres-stop',
    question: 'Что будет, если остановить PostgreSQL?',
    node: 'postgres',
    kind: 'stop',
    ttlS: 60,
    steps: [
      {
        text: 'PostgreSQL красный',
        why: 'База остановлена, соединения order-svc рвутся.',
        seen: (s) => stateOf(s, 'postgres') === 'broken',
      },
      {
        text: 'Жёлтым стало всё до балансера',
        why: 'Запрос идёт синхронно: gateway ждёт order-svc, тот ждёт базу. Жёсткая зависимость тянет вверх всю цепочку.',
        seen: (s) => all(s, ['order-svc', ...gateways, 'traefik'], 'degraded'),
      },
      {
        text: 'Покупатели получают ошибки',
        why: 'Заказ нельзя записать, значит его нельзя и принять. Красные частицы идут от order-svc до самого балансера.',
        seen: customersSeeErrors,
      },
      {
        text: 'Цепочка позеленела',
        why: 'Пул соединений переподключился сам, рестарт сервисов не понадобился.',
        seen: (s) => healed(s, 'postgres') && all(s, ['order-svc', ...gateways], 'healthy'),
      },
    ],
    lesson: 'В синхронной цепочке отказ хранилища виден пользователю сразу. Сравните с кухней: там тот же stop дал только задержку.',
  },
  {
    id: 'redis-stop',
    question: 'Что будет, если пропадёт Redis?',
    node: 'redis',
    kind: 'stop',
    ttlS: 60,
    steps: [
      {
        text: 'Redis красный',
        why: 'От него зависят двое: order-svc хранит в нём кеш меню, payment-svc хранит ключи идемпотентности.',
        seen: (s) => stateOf(s, 'redis') === 'broken',
      },
      {
        text: 'order-svc жёлтый, но меню отдаётся',
        why: 'Кеш это мягкая зависимость: при ошибке order-svc читает меню из базы. Медленнее, но работает.',
        seen: (s) => stateOf(s, 'order-svc') === 'degraded',
      },
      {
        text: 'Предохранитель оплаты открылся',
        why: 'Без ключа идемпотентности платёж можно списать дважды, поэтому payment-svc отказывает. Для него Redis жёсткая зависимость.',
        seen: (s) => breaker(s) === 'open',
      },
      {
        text: 'Оплата снова проходит',
        why: 'Пробный запрос в состоянии half-open прошёл, предохранитель закрылся.',
        seen: (s) => healed(s, 'redis') && breaker(s) === 'closed',
      },
    ],
    lesson: 'Один и тот же компонент бывает мягкой зависимостью для одного сервиса и жёсткой для другого. Это решает код клиента, а не сам компонент.',
  },
  {
    id: 'payment-errors',
    question: 'Что будет, если платёжка начнёт отвечать ошибками?',
    node: 'payment-svc',
    kind: 'app-error',
    ttlS: 60,
    steps: [
      {
        text: 'payment-svc красный, хотя контейнер жив',
        why: 'Процесс работает и проходит health-check, но 90% ответов это ошибки.',
        seen: (s) => stateOf(s, 'payment-svc') === 'broken',
      },
      {
        text: 'На ребре появилось «CB open»',
        why: 'После пяти неудач подряд order-svc перестал ходить в платёжку и отказывает сразу, не тратя полторы секунды на таймаут.',
        seen: (s) => breaker(s) === 'open',
      },
      {
        text: 'order-svc пожелтел по метрикам',
        why: 'Все контейнеры живы, но доля ошибок на ребре выше 20%. Контроллер видит это в Prometheus.',
        seen: (s) => stateOf(s, 'order-svc') === 'degraded',
      },
      {
        text: 'Предохранитель закрылся',
        why: 'Раз в 20 секунд он пропускает один пробный запрос. Первый успешный вернул обычный режим.',
        seen: (s) => healed(s, 'payment-svc') && breaker(s) === 'closed',
      },
    ],
    lesson: 'Circuit breaker не чинит зависимость. Он не даёт её поломке съесть ресурсы вызывающего и делает отказ быстрым.',
  },
  {
    id: 'order-latency',
    question: 'Что будет, если сеть до order-svc станет медленной?',
    node: 'order-svc',
    kind: 'latency',
    ttlS: 90,
    steps: [
      {
        text: 'order-svc помечен красным',
        why: 'К каждому его пакету добавлена задержка через tc netem. Контейнер при этом здоров.',
        seen: (s) => stateOf(s, 'order-svc') === 'broken',
      },
      {
        text: 'p95 на ребре gateway → order-svc выше секунды',
        why: 'Наведите курсор на ребро. Перцентиль считается за минуту, поэтому растёт не сразу.',
        seen: (s) => gateways.some((g) => (edgeOf(s, g, 'order-svc')?.p95_ms ?? 0) > SLOW_P95_MS),
      },
      {
        text: 'gateway пожелтели без единого упавшего контейнера',
        why: 'Медленный ответ для пользователя такой же отказ. Health-check этого не видит, метрики видят.',
        seen: (s) => gateways.some((g) => stateOf(s, g) === 'degraded'),
      },
      {
        text: 'Задержка ушла',
        why: 'Жёлтый держится ещё около минуты после починки: p95 помнит медленные запросы, пока они не выйдут из окна.',
        seen: (s) => healed(s, 'order-svc') && all(s, gateways, 'healthy'),
      },
    ],
    lesson: 'Медленно это тоже сломано. Проверки «процесс жив» такого не ловят, нужны метрики задержки.',
  },
];

/** A scenario is offered only while the topology still declares its fault. */
export function availableScenarios(topology: Topology): Scenario[] {
  return SCENARIOS.filter((sc) => topology.nodes.some((n) => n.id === sc.node && n.faults.includes(sc.kind)));
}

/**
 * Steps are a story, so they tick strictly in order: a later effect counts
 * only after the earlier ones were seen. Returns the new number of done steps.
 */
export function advance(done: number, scenario: Scenario, now: Snapshot, baseline: Snapshot): number {
  let next = done;
  while (next < scenario.steps.length && scenario.steps[next].seen(now, baseline)) next += 1;
  return next;
}
