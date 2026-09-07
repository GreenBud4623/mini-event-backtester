#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using Time = long long;
using OrderId = long long;
using OwnerId = long long;
using Price = long long;
using Quantity = long long;

enum class Side { Buy, Sell };
enum class EventAction { Add, Cancel, Modify };

struct MarketEvent {
    Time ts = 0;
    EventAction action = EventAction::Add;
    OrderId orderId = 0;
    OwnerId ownerId = 0;
    Side side = Side::Buy;
    Quantity quantity = 0;
    Price price = 0;
};

struct TopOfBook {
    Price bestBid = 0;
    Quantity bestBidQuantity = 0;
    Price bestAsk = 0;
    Quantity bestAskQuantity = 0;
};

struct StrategyConfig {
    double bias = 0.0;
    double imbalanceWeight = 0.0;
    double baseEdgeTicks = 0.0;
    double inventoryPenaltyTicks = 0.0;
    Quantity maxPosition = 0;
};

struct LatencyConfig {
    Time marketCommunicationTicks = 0;
    Time predictionGenerationTicks = 0;
    Time predictionTransferTicks = 0;
    double millisecondsPerTick = 10.0;
};

struct BacktestConfig {
    double feeTicksPerUnit = 0.0;
    double notionalFeeRate = 0.0;
    Quantity maxOrderSize = 1;
    Time maxCommandAgeTicks = 1000;
    LatencyConfig latency;
};

struct OrderCommand {
    Side side = Side::Buy;
    Quantity quantity = 1;
    Price limitPrice = 0;
};

struct StrategyDecision {
    std::optional<OrderCommand> command;
    double fairPrice = 0.0;
    double edge = 0.0;
};

struct PendingCommand {
    Time decisionTs = 0;
    Time executeAtTs = 0;
    Time latencyTicks = 0;
    OrderCommand command;
    double fairPrice = 0.0;
    double edge = 0.0;
};

struct Trade {
    Time decisionTs = 0;
    Time executeTs = 0;
    Time latencyTicks = 0;
    std::string action;
    Price limitPrice = 0;
    Price tradePrice = 0;
    double fairPrice = 0.0;
    double edge = 0.0;
    Quantity positionAfter = 0;
    double cashAfter = 0.0;
    double equityAfter = 0.0;
};

struct BacktestResult {
    int marketSnapshots = 0;
    int commands = 0;
    int rejectedCommands = 0;
    int missedCommands = 0;
    int staleCommands = 0;
    int droppedCommands = 0;
    std::vector<Trade> trades;
    Quantity finalPosition = 0;
    double finalMark = 0.0;
    double finalPnlTicks = 0.0;
    double minEquityTicks = 0.0;
    double maxEquityTicks = 0.0;
    double totalStrategyLatencyMs = 0.0;
    double maxStrategyLatencyMs = 0.0;
    Time maxStrategyLatencyTicks = 0;
};

struct Scenario {
    std::string name;
    BacktestConfig config;
};

// input ==> side(buy/sell)
Side parseSide(const std::string& value) {
    if (value == "BUY") {
        return Side::Buy;
    }
    if (value == "SELL") {
        return Side::Sell;
    }
    throw std::runtime_error("unknown side: " + value);
}

// input ==> actiune valida
EventAction parseAction(const std::string& value) {
    if (value == "ADD") {
        return EventAction::Add;
    }
    if (value == "CANCEL") {
        return EventAction::Cancel;
    }
    if (value == "MODIFY") {
        return EventAction::Modify;
    }
    throw std::runtime_error("unknown action: " + value);
}

// citesc tot documentul cu historical data
std::vector<MarketEvent> loadEventsFromFile(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("could not open event file: " + path);
    }

    std::vector<MarketEvent> events;
    std::string line;
    int lineNumber = 0;
    while (std::getline(input, line)) {
        ++lineNumber;
        if (line.empty()) {
            continue;
        }

        std::istringstream stream(line);
        std::string actionText;
        std::string sideText;
        MarketEvent event;
        if (!(stream >> event.ts >> actionText >> event.orderId >> event.ownerId >>
              sideText >> event.quantity >> event.price)) {
            throw std::runtime_error("invalid event at line " + std::to_string(lineNumber));
        }

        std::string unexpected;
        if (stream >> unexpected) {
            throw std::runtime_error("extra data at line " + std::to_string(lineNumber));
        }
        if (event.ts < 0 || event.orderId <= 0 || event.ownerId <= 0) {
            throw std::runtime_error("invalid identifier or time at line " + std::to_string(lineNumber));
        }

        event.action = parseAction(actionText);
        event.side = parseSide(sideText);
        if (event.action != EventAction::Cancel && (event.quantity <= 0 || event.price <= 0)) {
            throw std::runtime_error("add/modify requires positive quantity and price at line " +
                                     std::to_string(lineNumber));
        }
        events.push_back(event);
    }

    if (events.empty()) {
        throw std::runtime_error("event file is empty: " + path);
    }
    return events;
}

// mijloc intre best bid si best ask
double midPrice(const TopOfBook& top) {
    return (static_cast<double>(top.bestBid) + static_cast<double>(top.bestAsk)) / 2.0;
}

// transform secunde in tickuri
Time millisecondsToTicks(double elapsedMilliseconds, double millisecondsPerTick) {
    if (millisecondsPerTick <= 0.0) {
        throw std::runtime_error("millisecondsPerTick must be positive");
    }
    if (elapsedMilliseconds < millisecondsPerTick) {
        return 0;
    }
    return static_cast<Time>(std::ceil(elapsedMilliseconds / millisecondsPerTick));
}

// tin minte evenimente cu timestamp si pastrez ordinea
template <typename Event>
class TimeOrderedEventPool {
private:
    struct QueuedEvent {
        Event event;
        long long sequence = 0;
    };

    struct CompareQueuedEvent {
        // face std::priority_queue ai sa dea mai intai cel mai vechi timestamp
        bool operator()(const QueuedEvent& left, const QueuedEvent& right) const {
            if (left.event.ts != right.event.ts) {
                return left.event.ts > right.event.ts;
            }
            return left.sequence > right.sequence;
        }
    };

public:
    // muta un event in coada si ii da un nr in secv de insert
    void add(Event event) {
        events_.push(QueuedEvent{std::move(event), nextSequence_++});
    }

    // spune daca mai sunt evenimente neprocesate
    bool empty() const {
        return events_.empty();
    }

    // sterge si returneaza toate eventurile de la cel mai vechi timestamp
    std::vector<Event> popNextBatch() {
        std::vector<Event> batch;
        if (events_.empty()) {
            return batch;
        }

        const Time earliest = events_.top().event.ts;
        while (!events_.empty() && events_.top().event.ts == earliest) {
            batch.push_back(events_.top().event);
            events_.pop();
        }
        return batch;
    }

private:
    long long nextSequence_ = 0;
    std::priority_queue<QueuedEvent, std::vector<QueuedEvent>, CompareQueuedEvent> events_;
};

// scrie mesaje de tip log
class Logger {
public:
    void info(const std::string& message) const {
        std::cerr << "[INFO] " << message << '\n';
    }

    // tine un warning
    void warn(const std::string& message) const {
        std::cerr << "[WARN] " << message << '\n';
    }
};


///-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


// Tin orderuri necompletste si da date strategiei
class Book {
private:
    struct RestingOrder {
        OrderId id = 0;
        Quantity quantity = 0;
    };

    using PriceLevel = std::list<RestingOrder>;
    using AskLevels = std::map<Price, PriceLevel>;
    using BidLevels = std::map<Price, PriceLevel, std::greater<Price>>;

public:

    // Octavian : la process ar fi fain sa ai functie separata pentru modify; si aia sa apeleze un cancel si dupa un add

    // aplic add cancel sau modify
    void process(const MarketEvent& event) {
        if (event.action == EventAction::Cancel) {
            cancelOrder(event.orderId);
            return;
        }
        if (event.action == EventAction::Modify) {
            cancelOrder(event.orderId);
        }
        addOrder(event.orderId, event.side, event.quantity, event.price);
    }

    // returneaza cantitatea de oferte de la best bid / best ask
    std::optional<TopOfBook> topOfBook() const {
        if (bids_.empty() || asks_.empty()) {
            return std::nullopt;
        }
        return TopOfBook{
            bids_.begin()->first,
            sumLevel(bids_.begin()->second),
            asks_.begin()->first,
            sumLevel(asks_.begin()->second),
        };
    }

    // Octavian : pune comentariul cu prioritati sub : da fill...

    // da fill la un limit order prioritati : pret si timp
    std::optional<Price> executeLimitOrderIfMarketable(
        Side side,
        Quantity quantity,
        Price limitPrice
    ) {
        if (quantity <= 0 || limitPrice <= 0) {
            throw std::runtime_error("strategy order requires positive quantity and price");
        }
        if (side == Side::Buy) {
            return fillFromLevels(asks_, quantity, limitPrice, side);
        }
        return fillFromLevels(bids_, quantity, limitPrice, side);
    }

private:
    BidLevels bids_;
    AskLevels asks_;

    // Octavian : merita sa faci pair-ul un nume in sine; sa faci struct si sa scrii hash de mana sau il faci map normal si scrii comparator custom
    // like daca dau hover la .first / .second imi arata tipul da e mai nice sa vad direct
    ///si probabil Vlad ar comenta de asta so don't blame me
    std::unordered_map<OrderId, std::pair<Side, Price>> locations_;

    // Octavian : care e scopu sa mai dai cancel order? ar fi mai ok sa nu faci nimic daca ai doua order id-uri la fel
    // acum cand citesti datele din fisier, poti face checker la fisier de asta
    // sau sa intorci eroare maybe?

    // adauga un order nou la coada listei de pret
    void addOrder(OrderId orderId, Side side, Quantity quantity, Price price) {
        if (quantity <= 0 || price <= 0) {
            return;
        }
        cancelOrder(orderId);
        if (side == Side::Buy) {
            bids_[price].push_back(RestingOrder{orderId, quantity});
        } else {
            asks_[price].push_back(RestingOrder{orderId, quantity});
        }
        locations_[orderId] = {side, price};
    }

    // da remove la un order si ignora alte canceluri
    void cancelOrder(OrderId orderId) {
        const auto location = locations_.find(orderId);
        if (location == locations_.end()) {
            return;
        }

        const Side side = location->second.first;
        const Price price = location->second.second;
        locations_.erase(location);
        if (side == Side::Buy) {
            eraseOrder(bids_, price, orderId);
        } else {
            eraseOrder(asks_, price, orderId);
        }
    }

    // sterge un order de la o lista de preturi si sterge si lista cand e goala
    template <typename Levels>
    static void eraseOrder(Levels& levels, Price price, OrderId orderId) {
        const auto level = levels.find(price);
        if (level == levels.end()) {
            return;
        }
        level->second.remove_if([orderId](const RestingOrder& order) {
            return order.id == orderId;
        });
        if (level->second.empty()) {
            levels.erase(level);
        }
    }

    /// Octavian : sumLevel merita optimizat

    // aduna currenctul care inca e activ de la o lista de pret
    static Quantity sumLevel(const PriceLevel& level) {
        Quantity total = 0;
        for (const RestingOrder& order : level) {
            total += order.quantity;
        }
        return total;
    }

    // verifica daca e posibil un fill inainte sa mute currency
    template <typename Levels>
    static bool hasMarketableQuantity(
        const Levels& levels,
        Quantity requested,
        Price limitPrice,
        Side incomingSide
    ) {
        Quantity available = 0;
        for (const auto& [price, level] : levels) {
            if ((incomingSide == Side::Buy && price > limitPrice) ||
                (incomingSide == Side::Sell && price < limitPrice)) {
                break;
            }
            available += sumLevel(level);
            if (available >= requested) {
                return true;
            }
        }
        return false;
    }

    // consume listele de prtet si returneaza pretiul mediu de executie ponderat cu crrency
    template <typename Levels>
    std::optional<Price> fillFromLevels(
        Levels& levels,
        Quantity quantity,
        Price limitPrice,
        Side incomingSide
    ) {
        if (!hasMarketableQuantity(levels, quantity, limitPrice, incomingSide)) {
            return std::nullopt;
        }

        Quantity remaining = quantity;
        long long totalNotional = 0;
        while (remaining > 0) {
            auto level = levels.begin();
            const Price price = level->first;
            for (auto order = level->second.begin();
                 order != level->second.end() && remaining > 0;) {
                const Quantity fillQuantity = std::min(remaining, order->quantity);
                remaining -= fillQuantity;
                order->quantity -= fillQuantity;
                totalNotional += fillQuantity * price;
                if (order->quantity == 0) {
                    locations_.erase(order->id);
                    order = level->second.erase(order);
                } else {
                    ++order;
                }
            }
            if (level->second.empty()) {
                levels.erase(level);
            }
        }
        return totalNotional / quantity;
    }
};

// urmareste banii strategiei
class Portfolio {
public:
    // presupunerile pt taxe
    Portfolio(double feeTicksPerUnit, double notionalFeeRate)
        : feeTicksPerUnit_(feeTicksPerUnit), notionalFeeRate_(notionalFeeRate) {
        if (feeTicksPerUnit < 0.0 || notionalFeeRate < 0.0) {
            throw std::runtime_error("fees cannot be negative");
        }
    }

    // aplica buy fill decrementand banii si incrementand actiunile(pozitia)
    void buy(Price price, Quantity quantity) {
        cash_ -= static_cast<double>(price * quantity) + transactionFee(price, quantity);
        position_ += quantity;
    }

    // aplica sell fill --> bani++ si pozitie--
    void sell(Price price, Quantity quantity) {
        cash_ += static_cast<double>(price * quantity) - transactionFee(price, quantity);
        position_ -= quantity;
    }

    // returneaza inventarul curent
    Quantity position() const {
        return position_;
    }

    // returneaza banii
    double cash() const {
        return cash_;
    }

    // transforma(teoretic) inventarul in cash si il ahuna cu banii ca sa vada cat de mult a urcat
    double equity(double markPrice) const {
        return cash_ + static_cast<double>(position_) * markPrice;
    }

private:
    double feeTicksPerUnit_ = 0.0;
    double notionalFeeRate_ = 0.0;
    double cash_ = 0.0;
    Quantity position_ = 0;

    // calculeaz toate fee-urile per fill
    double transactionFee(Price price, Quantity quantity) const {
        const double notional = static_cast<double>(price * quantity);
        return feeTicksPerUnit_ * static_cast<double>(quantity) +
               notional * notionalFeeRate_;
    }
};

// defineste strategia
class Strategy {
public:
    virtual ~Strategy() = default;

    // produce max o comanda de la stareapublica si inventar
    virtual StrategyDecision onTimeMove(
        const TopOfBook& top,
        Quantity effectivePosition
    ) const = 0;
};

// foloseste inventarul si thresholdurile pt a da trade
class FairPriceStrategy final : public Strategy {
public:
    // tine minte modelul antrenat
    explicit FairPriceStrategy(StrategyConfig config) : config_(config) {}

    // calculeaza preturile fair si verifica marja de profit si de vanzare si returneaza o decizie
    StrategyDecision onTimeMove(
        const TopOfBook& top,
        Quantity effectivePosition
    ) const override {
        const Quantity totalQuantity = top.bestBidQuantity + top.bestAskQuantity;
        if (totalQuantity <= 0) {
            return {};
        }

        const double imbalance =
            static_cast<double>(top.bestBidQuantity - top.bestAskQuantity) /
            static_cast<double>(totalQuantity);
        const double fairPrice =
            midPrice(top) + config_.bias + config_.imbalanceWeight * imbalance;
        const double buyEdge = fairPrice - static_cast<double>(top.bestAsk);
        const double sellEdge = static_cast<double>(top.bestBid) - fairPrice;
        const double requiredBuyEdge =
            config_.baseEdgeTicks + effectivePosition * config_.inventoryPenaltyTicks;
        const double requiredSellEdge =
            config_.baseEdgeTicks - effectivePosition * config_.inventoryPenaltyTicks;

        if (effectivePosition < config_.maxPosition && buyEdge >= requiredBuyEdge) {
            return StrategyDecision{OrderCommand{Side::Buy, 1, top.bestAsk}, fairPrice, buyEdge};
        }
        if (effectivePosition > -config_.maxPosition && sellEdge >= requiredSellEdge) {
            return StrategyDecision{OrderCommand{Side::Sell, 1, top.bestBid}, fairPrice, sellEdge};
        }
        return StrategyDecision{std::nullopt, fairPrice, 0.0};
    }

private:
    StrategyConfig config_;
};

// aplica reguli de siguranta independente de strategie
class RiskManager {
public:
    // tine minte limitele folosite la fiecare comanda
    RiskManager(Quantity maxOrderSize, Quantity maxPosition)
        : maxOrderSize_(maxOrderSize), maxPosition_(maxPosition) {}

    // da reject la size-uri, preturi si pozitii invalide
    bool allow(const OrderCommand& command, Quantity effectivePosition) const {
        if (command.quantity <= 0 || command.quantity > maxOrderSize_ || command.limitPrice <= 0) {
            return false;
        }
        const Quantity signedQuantity =
            command.side == Side::Buy ? command.quantity : -command.quantity;
        const Quantity projectedPosition = effectivePosition + signedQuantity;
        return std::abs(projectedPosition) <= maxPosition_;
    }

private:
    Quantity maxOrderSize_ = 0;
    Quantity maxPosition_ = 0;
};

// tine starea simularii si face eventurile, deciziile, latency-ulsi fillurile
class Backtest {
public:
    // tine strategia si copiaza scriptul
    Backtest(
        std::vector<MarketEvent> events,
        std::unique_ptr<Strategy> strategy,
        StrategyConfig strategyConfig,
        BacktestConfig config,
        const Logger& logger
    )
        : strategy_(std::move(strategy)),
          config_(config),
          riskManager_(config.maxOrderSize, strategyConfig.maxPosition),
          portfolio_(config.feeTicksPerUnit, config.notionalFeeRate),
          logger_(logger) {
        for (MarketEvent& event : events) {
            eventPool_.add(std::move(event));
        }
    }

    // da run la toate timestampurile si returneaza metrici de performanta
    BacktestResult run() {
        BacktestResult result;
        Time lastTs = 0;
        while (!eventPool_.empty()) {
            const std::vector<MarketEvent> batch = eventPool_.popNextBatch();
            const Time ts = batch.front().ts;
            lastTs = ts;
            for (const MarketEvent& event : batch) {
                book_.process(event);
            }

            executeDueCommands(ts, result);
            const std::optional<TopOfBook> top = book_.topOfBook();
            if (!top.has_value()) {
                continue;
            }

            ++result.marketSnapshots;
            result.finalMark = midPrice(*top);
            requestStrategyCommand(ts, *top, result);
            executeDueCommands(ts, result);
            updateEquityStats(*top, result);
        }

        for (const PendingCommand& pending : pendingCommands_) {
            if (pending.executeAtTs > lastTs) {
                ++result.droppedCommands;
            }
        }
        result.finalPosition = portfolio_.position();
        result.finalPnlTicks = portfolio_.equity(result.finalMark);
        return result;
    }

private:
    std::unique_ptr<Strategy> strategy_;
    BacktestConfig config_;
    RiskManager riskManager_;
    Portfolio portfolio_;
    const Logger& logger_;
    Book book_;
    TimeOrderedEventPool<MarketEvent> eventPool_;
    std::vector<PendingCommand> pendingCommands_;

    // insumeaza inventarul
    Quantity inFlightPosition() const {
        Quantity total = 0;
        for (const PendingCommand& pending : pendingCommands_) {
            total += pending.command.side == Side::Buy
                         ? pending.command.quantity
                         : -pending.command.quantity;
        }
        return total;
    }

    // verifica strategia si riscurile
    void requestStrategyCommand(Time ts, const TopOfBook& top, BacktestResult& result) {
        const Quantity effectivePosition = portfolio_.position() + inFlightPosition();
        const auto start = std::chrono::steady_clock::now();
        const StrategyDecision decision = strategy_->onTimeMove(top, effectivePosition);
        const auto end = std::chrono::steady_clock::now();
        const double elapsedMs =
            std::chrono::duration<double, std::milli>(end - start).count();
        const Time measuredTicks = millisecondsToTicks(
            elapsedMs,
            config_.latency.millisecondsPerTick
        );
        result.totalStrategyLatencyMs += elapsedMs;
        result.maxStrategyLatencyMs = std::max(result.maxStrategyLatencyMs, elapsedMs);
        result.maxStrategyLatencyTicks = std::max(result.maxStrategyLatencyTicks, measuredTicks);

        if (!decision.command.has_value()) {
            return;
        }
        if (!riskManager_.allow(*decision.command, effectivePosition)) {
            ++result.rejectedCommands;
            logger_.warn("risk manager rejected a command at t=" + std::to_string(ts));
            return;
        }

        const Time totalLatency =
            config_.latency.marketCommunicationTicks +
            config_.latency.predictionGenerationTicks +
            config_.latency.predictionTransferTicks + measuredTicks;
        pendingCommands_.push_back(PendingCommand{
            ts,
            ts + totalLatency,
            totalLatency,
            *decision.command,
            decision.fairPrice,
            decision.edge,
        });
        ++result.commands;
    }

    // executa comenzile da predictii si numara lichiditate pierduta
    void executeDueCommands(Time ts, BacktestResult& result) {
        std::vector<PendingCommand> stillPending;
        for (const PendingCommand& pending : pendingCommands_) {
            if (pending.executeAtTs > ts) {
                stillPending.push_back(pending);
                continue;
            }
            if (ts - pending.decisionTs > config_.maxCommandAgeTicks) {
                ++result.staleCommands;
                continue;
            }

            const std::optional<Price> tradePrice = book_.executeLimitOrderIfMarketable(
                pending.command.side,
                pending.command.quantity,
                pending.command.limitPrice
            );
            if (!tradePrice.has_value()) {
                ++result.missedCommands;
                continue;
            }
            recordFill(ts, pending, *tradePrice, result);
        }
        pendingCommands_ = std::move(stillPending);
    }

    // aplica un fill si salveaza trade record
    void recordFill(
        Time ts,
        const PendingCommand& pending,
        Price tradePrice,
        BacktestResult& result
    ) {
        std::string action;
        if (pending.command.side == Side::Buy) {
            portfolio_.buy(tradePrice, pending.command.quantity);
            action = "BUY";
        } else {
            portfolio_.sell(tradePrice, pending.command.quantity);
            action = "SELL";
        }

        const std::optional<TopOfBook> topAfterTrade = book_.topOfBook();
        const double mark = topAfterTrade.has_value()
                                ? midPrice(*topAfterTrade)
                                : static_cast<double>(tradePrice);
        result.trades.push_back(Trade{
            pending.decisionTs,
            ts,
            pending.latencyTicks,
            action,
            pending.command.limitPrice,
            tradePrice,
            pending.fairPrice,
            pending.edge,
            portfolio_.position(),
            portfolio_.cash(),
            portfolio_.equity(mark),
        });
    }

    // updateaza observarile despre strat dupa fiecare snapshot
    void updateEquityStats(const TopOfBook& fallbackTop, BacktestResult& result) const {
        const std::optional<TopOfBook> currentTop = book_.topOfBook();
        const double mark = currentTop.has_value() ? midPrice(*currentTop) : midPrice(fallbackTop);
        result.finalMark = mark;
        const double equity = portfolio_.equity(mark);
        result.minEquityTicks = std::min(result.minEquityTicks, equity);
        result.maxEquityTicks = std::max(result.maxEquityTicks, equity);
    }
};

// printeaza un csv ca sa se compare rezultate
void printResult(const Scenario& scenario, const BacktestResult& result) {
    std::cout
        << scenario.name << ','
        << scenario.config.latency.marketCommunicationTicks << ','
        << scenario.config.latency.predictionGenerationTicks << ','
        << scenario.config.latency.predictionTransferTicks << ','
        << scenario.config.notionalFeeRate << ','
        << scenario.config.maxCommandAgeTicks << ','
        << result.marketSnapshots << ','
        << result.commands << ','
        << result.trades.size() << ','
        << result.rejectedCommands << ','
        << result.missedCommands << ','
        << result.staleCommands << ','
        << result.droppedCommands << ','
        << result.finalPosition << ','
        << result.finalMark << ','
        << result.finalPnlTicks << ','
        << result.minEquityTicks << ','
        << result.maxEquityTicks << ','
        << result.maxStrategyLatencyMs << ','
        << result.maxStrategyLatencyTicks
        << '\n';
}

// incarca streamul de evenimente, rulez scenarii si raporteaza erorile
int main(int argc, char* argv[]) {
    try {
        const std::string eventPath = argc > 1 ? argv[1] : "lesson07_simulation_events.txt";
        const StrategyConfig strategyConfig{
            -0.482072524654,
            8.509332824319,
            6.0,
            1.0,
            6,
        };
        const std::vector<MarketEvent> events = loadEventsFromFile(eventPath);
        const Logger logger;
        logger.info("loaded " + std::to_string(events.size()) + " events from " + eventPath);

        const std::vector<Scenario> scenarios{
            {"no_added_latency", {0.05, 0.0, 1, 1000, {0, 0, 0, 10.0}}},
            {"market_only_1_tick", {0.05, 0.0, 1, 1000, {1, 0, 0, 10.0}}},
            {"teaching_default_4_ticks", {0.05, 0.0, 1, 1000, {1, 2, 1, 10.0}}},
            {"pessimistic_9_ticks", {0.05, 0.0, 1, 1000, {2, 5, 2, 10.0}}},
            {"notional_fee_1_basis_point", {0.05, 0.0001, 1, 1000, {0, 0, 0, 10.0}}},
            {"stale_guard_3_ticks", {0.05, 0.0, 1, 3, {1, 2, 1, 10.0}}},
        };

        std::cout << std::fixed << std::setprecision(4);
        std::cout
            << "scenario,marketLatencyTicks,predictionGenerationTicks,"
            << "predictionTransferTicks,notionalFeeRate,maxCommandAgeTicks,"
            << "snapshots,commands,trades,rejectedCommands,missedCommands,"
            << "staleCommands,droppedCommands,finalPosition,finalMark,finalPnlTicks,"
            << "minEquityTicks,maxEquityTicks,maxStrategyLatencyMs,maxStrategyLatencyTicks\n";

        for (const Scenario& scenario : scenarios) {
            auto strategy = std::make_unique<FairPriceStrategy>(strategyConfig);
            Backtest backtest(events, std::move(strategy), strategyConfig, scenario.config, logger);
            printResult(scenario, backtest.run());
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[ERROR] " << error.what() << '\n';
        return 1;
    }
}
