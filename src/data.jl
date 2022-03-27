import REPL

using REPL.TerminalMenus
using CSV
using DataFrames, Query

export rebalance

function list_csv(path::String)
  files = [file for file in readdir(path) if isfile(abspath(joinpath(path, file)))]
  files = [m.match for m in match.(r".*\.csv", files) if m != nothing]
  return (path * "/").*(files)
end

function choose(prompt, options)
  menu = RadioMenu(options)
  choice = request(prompt, menu)
  return options[choice]
end

function request_csv(path::String, prompt::String)
  options = list_csv(path)
  return choose(prompt, options)
end

function open_fidelity_csv(path::String=homedir() * "/Downloads")
  filename = request_csv(path, "Open Fidelity CSV:")
  df = DataFrame(CSV.File(filename; footerskip=6, normalizenames=true, stringtype=String))
  return df[:,2:8]
end

function open_strategy_csv(path::String=homedir() * "/Documents")
  filename = request_csv(path, "Open Targets CSV:")
  return DataFrame(CSV.File(filename; normalizenames=true, stringtype=String))
end

function by_account(accounts::DataFrame, name::String)
  @from a in accounts begin
    @where a.Account_Name == name
    @select a
    @collect DataFrame
  end
end

function get_account(accounts::DataFrame)
  account_names = @from a in accounts begin
    @group a by a.Account_Name into n
    @select {Account_Name=key(n)}
    @collect DataFrame
  end
  account_name = choose("Choose account:", account_names[:, 1])
  return by_account(accounts, account_name)
end

function target_symbol(taxable::Bool, target)
  if taxable
    return target.Symbol_Tax
  else
    return target.Symbol_Adv
  end
end

function make_target(strategy::DataFrame)
  taxable = ("Taxable" == choose("Account Type:", ["Taxable", "Tax-Advantaged"]))
  levels = ["Conservative", "Moderate", "Balanced", "Growth/Income", "Growth", "Aggressive"]
  target = choose("Target:", levels)
  sort!(strategy, :Target, rev=true)
  # starts at 30%
  equity_percent = (findfirst(t -> t == target, levels) + 2) * 10
  equities = @from t in strategy begin
    @where !Bool(t.Income)
    @select {Symbol=target_symbol(taxable, t), t.Target}
    @collect DataFrame
  end
  equities.Target = allocate(equity_percent, equities.Target)
  income = @from t in strategy begin
    @where Bool(t.Income)
    @select {Symbol=target_symbol(taxable, t), t.Target}
    @collect DataFrame
  end
  income_percent = 100 - equity_percent
  income.Target = allocate(income_percent, income.Target)
  return sort(vcat(equities, income), :Target, rev=true)
end

function unshift(trades::DataFrame)
  return trades[1, :], trades[2:nrow(trades), :]
end

function is_core(symbol)
  return endswith(symbol, "**") || symbol == "Pending Activity"
end

function Base.get(a::String, b)
  return a
end

function sum_dtoi(c::Vector{String})
  return sum(dtoi.(c))
end

function sum_dtoi(c::Vector{Union{Missing, String}})
  result = 0
  for d in c
    result += dtoi(get(d, "\$0.00"))
  end
  return result
end

function get_core(account::DataFrame)
  core = @from asset in account begin
    @select {asset.Symbol, Current_Value=get(asset.Current_Value, asset.Last_Price_Change)}
    @collect DataFrame
  end
  return filter(asset -> is_core(get(asset.Symbol, "?")), core)
end

function get_non_core(account::DataFrame)
  nonCore = @from asset in account begin
    @select {asset.Symbol, asset.Description, asset.Quantity, asset.Current_Value}
    @collect DataFrame
  end
  return filter(asset -> !is_core(get(asset.Symbol, "?")), nonCore)
end

function get_holdings(target::DataFrame, nonCore::DataFrame)
  return @from asset in leftjoin(target, nonCore, on = :Symbol) begin
    @orderby descending(asset.Target), descending(dtoi(get(asset.Current_Value, "\$0.00")))
    @select {asset.Symbol, asset.Description, Quantity=get(asset.Quantity, 0.0), Current_Value=get(asset.Current_Value, "\$0.00"), asset.Target}
    @collect DataFrame
  end
end

function get_exiting(target::DataFrame, nonCore::DataFrame)
  exits = @from asset in nonCore begin
    @orderby descending(dtoi(get(asset.Current_Value, "\$0.00")))
    @select {asset.Symbol, asset.Description, asset.Quantity, asset.Current_Value, Target=0, Trade_Type="SELL", Trade_Value=itod(-dtoi(get(asset.Current_Value, "\$0.00"))), Drift=NaN, DriftPct=NaN}
    @collect DataFrame
  end
  return filter(e -> !(e.Symbol in target.Symbol), exits)
end

function generate_trades(account::DataFrame, target::DataFrame, deposit::Int=0)
  # core cash and pending
  core = get_core(account)
  println("\n---- HOLDINGS ----")
  if nrow(core) > 0
    printframe(core)
  end
  # assets other than core position
  nonCore = get_non_core(account)
  # account totals
  core_value = sum_dtoi(core.Current_Value)
  non_core_value = sum_dtoi(nonCore.Current_Value)
  cash = core_value + deposit
  accountTotal = core_value + non_core_value
  newTotal = non_core_value + cash
  # assets not in target (sell them)
  exiting = get_exiting(target, nonCore)
  # target assets (trade them)
  holdings = get_holdings(target, nonCore)
  # calculate drift from target
  holdings[!, :Drift] = map(h -> round(((dtoi(h.Current_Value)*100/accountTotal) - h.Target)*10, RoundNearestTiesAway)/10, eachrow(holdings))
  holdings[!, :DriftPct] = map(h -> round(h.Drift*100/h.Target*10)/10, eachrow(holdings))
  printframe(holdings)
  println("CASH | ", itod(core_value))
  # trade value in dollars
  tradeAmount = allocate(newTotal, holdings.Target[:,1]) - map(h -> dtoi(h), holdings.Current_Value[:,1])
  tradeType = map(t -> t < 0 ? "SELL" : "BUY", tradeAmount)
  holdings[!, :Trade_Type] = tradeType
  holdings[!, :Trade_Value] = itod.(tradeAmount)
  # combine all tradable holdings
  trades = vcat(holdings, exiting)
  # create exchange trades, sort
  first, second = optimize_trades(trades, cash)
  println("\n---- TRADES ----")
  settle = print_trades(first, core_value)
  if nrow(second) > 0
    settle = print_trades(second, settle)
  end
end

function optimize_trades(trades::DataFrame, cash::Int)
  # split trades into buys and sells
  sells = @from trade in trades begin
    @where trade.Trade_Type == "SELL"
    @orderby dtoi(trade.Trade_Value)
    @select {trade.Symbol, trade.Description, trade.Quantity, trade.Current_Value, trade.Trade_Type, trade.Trade_Value, Trade_For="CASH", Trade_Shares=NaN}
    @collect DataFrame
  end
  buys = @from trade in trades begin
    @where trade.Trade_Type == "BUY"
    @orderby descending(dtoi(trade.Trade_Value))
    @select {trade.Symbol, trade.Description, trade.Quantity, trade.Current_Value, trade.Trade_Type, trade.Trade_Value, Trade_For="", Trade_Shares=NaN}
    @collect DataFrame
  end
  # find mutual fund exchanges
  trades = similar(sells, 0)
  while true
    if nrow(sells) == 0
      # out of sells, append remaining buys
      append!(trades, buys)
      break
    end
    sell, sells = unshift(sells)
    if !endswith(get(sell.Symbol, "?"), "X")
      # not exchangable sell, push it
      push!(trades, sell)
      continue
    end
    if nrow(buys) == 0
      # out of buys, append remaining sells
      push!(trades, sell)
      append!(trades, sells)
      break
    end
    buy, buys = unshift(buys)
    if !endswith(get(buy.Symbol, "?"), "X")
      # not exchangable buy, push it
      push!(trades, buy)
      push!(sells, sell)
      sells = @from trade in sells begin
        @orderby dtoi(trade.Trade_Value)
        @select trade
        @collect DataFrame
      end
      continue
    end
    # exchange mutual funds
    if abs(dtoi(sell.Trade_Value)) >= dtoi(buy.Trade_Value)
      # sell larger than buy - reduce sell and turn buy into exchange
      sell.Trade_Value = itod(dtoi(sell.Trade_Value) + dtoi(buy.Trade_Value))
      buy.Trade_For = buy.Symbol
      buy.Symbol = sell.Symbol
      buy.Description = sell.Description
      buy.Quantity = sell.Quantity
      buy.Current_Value = sell.Current_Value
      buy.Trade_Type = "EXCHANGE"
      buy.Trade_Value = itod(-dtoi(buy.Trade_Value))
      push!(sells, sell)
      push!(trades, buy)
      sells = @from trade in sells begin
        @orderby dtoi(trade.Trade_Value)
        @select trade
        @collect DataFrame
      end
    else
      # buy larger than sell, reduce buy and turn sell into exchange
      buy.Trade_Value = itod(dtoi(sell.Trade_Value) + dtoi(buy.Trade_Value))
      sell.Trade_Type = "EXCHANGE"
      sell.Trade_For = buy.Symbol
      push!(buys, buy)
      push!(trades, sell)
      buys = @from trade in buys begin
        @orderby descending(dtoi(trade.Trade_Value))
        @select trade
        @collect DataFrame
      end
    end
  end
  # filter-out zero-value trades
  trades = @from trade in trades begin
    @where dtoi(trade.Trade_Value) != 0
    @orderby trade.Symbol
    @select trade
    @collect DataFrame
  end
  # calculate number of shares for sells and exchanges
  symbol = ""
  remaining_quantity = 0
  remaining_value = 0
  for trade in eachrow(trades)
    if trade.Trade_Type == "BUY"
      # skip trade-quantity for buys
      trade.Trade_Shares = NaN
      trade.Trade_For = ""
      continue
    end
    if trade.Symbol != symbol
      # reset on next symbol
      symbol = trade.Symbol
      remaining_quantity = qtoi(trade.Quantity)
      remaining_value = dtoi(trade.Current_Value)
    end
    # value for this sell/exchange, and remainder
    trade_value = abs(dtoi(trade.Trade_Value))
    remaining_value -= trade_value
    if (remaining_value == 0)
      trade_quantity = remaining_quantity
    else
      # allocate shares in thousandths
      trade_quantity, remaining_quantity = allocate(remaining_quantity, [trade_value, remaining_value])
    end
    trade.Trade_Shares = itoq(trade_quantity)
  end
  # split trades into sells-first, and buys-second
  first = @from trade in trades begin
    @where trade.Trade_Type != "BUY"
    @orderby endswith(get(trade.Symbol, "?"), "XX"), descending(trade.Trade_Type), trade.Symbol, descending(abs(dtoi(trade.Trade_Value)))
    @select trade
    @collect DataFrame
  end
  second = @from trade in trades begin
    @where trade.Trade_Type == "BUY"
    # least-expensive buys first
    @orderby dtoi(trade.Trade_Value)
    @select trade
    @collect DataFrame
  end
  # non-mutual fund sales - for funding trades
  non_mutual = @from trade in first begin
    @where !endswith(get(trade.Symbol, "?"), "X")
    @select trade
    @collect DataFrame
  end
  # amount of funding for first trades
  available = max(0, cash) - sum_dtoi(non_mutual.Trade_Value)
  # move buys from second to first, if we can afford them
  while true
    if nrow(second) == 0
      break
    end
    # least-expensive buy...
    buy, second = unshift(second)
    if dtoi(buy.Trade_Value) <= available
      available -= dtoi(buy.Trade_Value)
      push!(first, buy)
    else
      push!(second, buy)
      break
    end
  end
  # final sort
  first = @from t in first begin
    @orderby endswith(get(t.Trade_For, "?"), "XX"), endswith(get(t.Symbol, "?"), "XX"), endswith(get(t.Symbol, "?"), "X"), descending(t.Trade_Type), descending(abs(dtoi(t.Trade_Value)))
    @select {t.Symbol, t.Trade_Type, t.Trade_Value, t.Trade_Shares, t.Trade_For}
    @collect DataFrame
  end
  second = @from t in second begin
    @orderby endswith(get(t.Symbol, "?"), "XX"), endswith(get(t.Symbol, "?"), "X"), descending(t.Trade_Type), descending(abs(dtoi(t.Trade_Value)))
    @select {t.Symbol, t.Trade_Type, t.Trade_Value, t.Trade_Shares, t.Trade_For}
    @collect DataFrame
  end
  return first, second
end

function printframe(df::DataFrame)
  println(replace(sprint(show, df, context=:compact=>false), r".*DataFrame" => ""))
end

function print_trades(trades::DataFrame, core::Int)
  printframe(trades)
  nonex = @from trade in trades begin
    @where trade.Trade_Type != "EXCHANGE"
    @select trade
    @collect DataFrame
  end
  settle = max(0, core) - sum_dtoi(nonex.Trade_Value)
  println("CASH | ", itod(settle))
  return settle
end

function rebalance(deposit::Int=0)
  accounts = open_fidelity_csv()
  strategy = open_strategy_csv()
  println()
  account = get_account(accounts)
  target = make_target(strategy)
  generate_trades(account, target, deposit)
end
