use std::collections::HashMap;

use anyhow::Result;
use crossterm::event::{self, Event as CEvent, EventStream, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use futures_util::StreamExt;
use ratatui::prelude::*;
use ratatui::symbols::Marker;
use ratatui::widgets::*;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

use crate::analysis::opportunity::{Action, Opportunity, RiskLevel};
use crate::analysis::portfolio::PortfolioOptimizer;
use crate::analysis::short_put_history::HistoryResolution;
use crate::analysis::short_put_history_service::{
    LoadedShortPutHistory, ShortPutHistoryRequest, ShortPutHistoryService,
};

pub enum TuiEvent {
    Opportunity(Opportunity),
    Connected {
        instrument_count: usize,
    },
    ShortPutHistoryLoaded {
        cache_key: String,
        history: LoadedShortPutHistory,
    },
    ShortPutHistoryFailed {
        cache_key: String,
        error: String,
    },
}

#[derive(PartialEq)]
enum View {
    List,
    Detail,
}

#[derive(PartialEq, Clone, Copy)]
enum Filter {
    All,
    Arbitrage,
    Signal,
    Pcp,
    Spread,
    ConvRev,
    Calendar,
    Vol,
    ShortPut,
    ShortCall,
    Portfolio,
}

#[derive(PartialEq, Clone, Copy)]
enum SortBy {
    Profit,
    Time,
    Apy,
    Expiry,
}

#[derive(PartialEq, Clone, Copy)]
enum ExpiryFilter {
    All,
    Short,  // ≤7 days
    Medium, // 7-30 days
    Long,   // >30 days
}

impl ExpiryFilter {
    fn label(self) -> &'static str {
        match self {
            ExpiryFilter::All => "All Expiry",
            ExpiryFilter::Short => "≤7d",
            ExpiryFilter::Medium => "7-30d",
            ExpiryFilter::Long => ">30d",
        }
    }

    fn matches(self, expiry_ms: Option<i64>) -> bool {
        match self {
            ExpiryFilter::All => true,
            _ => {
                let Some(expiry_ms) = expiry_ms else {
                    return false;
                };
                let now_ms = chrono::Utc::now().timestamp() * 1000;
                let days = (expiry_ms - now_ms) as f64 / 86_400_000.0;
                match self {
                    ExpiryFilter::Short => days <= 7.0,
                    ExpiryFilter::Medium => days > 7.0 && days <= 30.0,
                    ExpiryFilter::Long => days > 30.0,
                    ExpiryFilter::All => unreachable!(),
                }
            }
        }
    }

    fn next(self) -> Self {
        match self {
            ExpiryFilter::All => ExpiryFilter::Short,
            ExpiryFilter::Short => ExpiryFilter::Medium,
            ExpiryFilter::Medium => ExpiryFilter::Long,
            ExpiryFilter::Long => ExpiryFilter::All,
        }
    }
}

const LEVERAGE_OPTIONS: [f64; 4] = [1.0, 2.0, 5.0, 10.0];

struct App {
    opportunities: Vec<Opportunity>,
    opp_map: HashMap<String, usize>,
    filtered: Vec<usize>,
    view: View,
    filter: Filter,
    sort_by: SortBy,
    table_state: TableState,
    /// Track which opportunity is selected by key (stable across re-sorts)
    selected_key: Option<String>,
    should_quit: bool,
    connected: bool,
    instrument_count: usize,
    leverage_idx: usize,
    /// Portfolio combinations (recomputed periodically)
    portfolios: Vec<Opportunity>,
    portfolio_optimizer: PortfolioOptimizer,
    history_request_tx: Option<mpsc::UnboundedSender<ShortPutHistoryRequest>>,
    history_cache: HashMap<String, LoadedShortPutHistory>,
    pending_history_key: Option<String>,
    detail_resolution: HistoryResolution,
    detail_history_error: Option<String>,
    expiry_filter: ExpiryFilter,
}

impl App {
    fn new(history_request_tx: Option<mpsc::UnboundedSender<ShortPutHistoryRequest>>) -> Self {
        App {
            opportunities: Vec::new(),
            opp_map: HashMap::new(),
            filtered: Vec::new(),
            view: View::List,
            filter: Filter::All,
            sort_by: SortBy::Profit,
            table_state: TableState::default(),
            selected_key: None,
            should_quit: false,
            connected: false,
            instrument_count: 0,
            leverage_idx: 0,
            portfolios: Vec::new(),
            portfolio_optimizer: PortfolioOptimizer::new(1.0),
            history_request_tx,
            history_cache: HashMap::new(),
            pending_history_key: None,
            detail_resolution: HistoryResolution::OneHour,
            detail_history_error: None,
            expiry_filter: ExpiryFilter::All,
        }
    }

    fn leverage(&self) -> f64 {
        LEVERAGE_OPTIONS[self.leverage_idx]
    }

    fn opp_key(opp: &Opportunity) -> String {
        let mut instruments = opp.instruments.clone();
        instruments.sort();
        format!("{}:{}", opp.strategy_type, instruments.join(","))
    }

    fn history_cache_key(opp: &Opportunity, resolution: HistoryResolution) -> String {
        format!("{}|{}", Self::opp_key(opp), resolution.label())
    }

    fn add_opportunity(&mut self, opp: Opportunity) {
        let key = Self::opp_key(&opp);
        if let Some(&idx) = self.opp_map.get(&key) {
            self.opportunities[idx] = opp;
        } else {
            let idx = self.opportunities.len();
            self.opp_map.insert(key, idx);
            self.opportunities.push(opp);
        }
        self.update_filtered();
        if self.table_state.selected().is_none() && !self.filtered.is_empty() {
            self.table_state.select(Some(0));
        }
    }

    fn recompute_portfolios(&mut self) {
        self.portfolio_optimizer.set_leverage(self.leverage());
        self.portfolios = self.portfolio_optimizer.find_best(&self.opportunities, 10);
    }

    fn selected_opportunity(&self) -> Option<Opportunity> {
        self.table_state
            .selected()
            .and_then(|i| self.filtered.get(i))
            .map(|&idx| self.display_opps()[idx].clone())
    }

    fn current_detail_history(&self, opp: &Opportunity) -> Option<&LoadedShortPutHistory> {
        self.history_cache
            .get(&Self::history_cache_key(opp, self.detail_resolution))
    }

    fn request_history_for_current_selection(&mut self) {
        self.detail_history_error = None;

        let Some(opp) = self.selected_opportunity() else {
            return;
        };

        if opp.strategy_type != "short_put_yield" && opp.strategy_type != "short_call_yield" {
            self.pending_history_key = None;
            return;
        }

        let Some(expiry_timestamp_ms) = opp.expiry_timestamp else {
            self.detail_history_error = Some("missing expiry timestamp".to_string());
            self.pending_history_key = None;
            return;
        };

        let Some(instrument_name) = opp.instruments.first().cloned() else {
            self.detail_history_error = Some("missing option instrument".to_string());
            self.pending_history_key = None;
            return;
        };

        let cache_key = Self::history_cache_key(&opp, self.detail_resolution);
        if self.history_cache.contains_key(&cache_key) {
            self.pending_history_key = None;
            return;
        }

        let Some(history_request_tx) = &self.history_request_tx else {
            self.detail_history_error = Some("history loader unavailable".to_string());
            self.pending_history_key = None;
            return;
        };

        let request = ShortPutHistoryRequest {
            cache_key: cache_key.clone(),
            instrument_name,
            strike: opp.total_cost,
            expiry_timestamp_ms,
            resolution: self.detail_resolution,
        };

        if history_request_tx.send(request).is_err() {
            self.detail_history_error = Some("failed to queue history request".to_string());
            self.pending_history_key = None;
            return;
        }

        self.pending_history_key = Some(cache_key);
    }

    /// Get the display list: either main opportunities or portfolio combos
    fn display_opps(&self) -> &[Opportunity] {
        if self.filter == Filter::Portfolio {
            &self.portfolios
        } else {
            &self.opportunities
        }
    }

    fn update_filtered(&mut self) {
        // Remember current selection by key
        if let Some(sel) = self.table_state.selected() {
            if let Some(&idx) = self.filtered.get(sel) {
                let opps = self.display_opps();
                if idx < opps.len() {
                    self.selected_key = Some(Self::opp_key(&opps[idx]));
                }
            }
        }

        let now = chrono::Utc::now().timestamp();
        let stale_threshold = 60;

        if self.filter == Filter::Portfolio {
            // Portfolio mode: show all portfolio combos (already top 10)
            self.filtered = (0..self.portfolios.len()).collect();
        } else {
            self.filtered = self
                .opportunities
                .iter()
                .enumerate()
                .filter(|(_, opp)| {
                    if now - opp.detected_at > stale_threshold {
                        return false;
                    }
                    let type_match = match self.filter {
                        Filter::All => true,
                        Filter::Arbitrage => is_arb(&opp.strategy_type),
                        Filter::Signal => !is_arb(&opp.strategy_type),
                        Filter::Pcp => opp.strategy_type == "put_call_parity",
                        Filter::Spread => matches!(
                            opp.strategy_type.as_str(),
                            "vertical_arb" | "butterfly_arb" | "box_spread"
                        ),
                        Filter::ConvRev => {
                            matches!(opp.strategy_type.as_str(), "conversion" | "reversal")
                        }
                        Filter::Calendar => matches!(
                            opp.strategy_type.as_str(),
                            "calendar_arb" | "calendar_spread"
                        ),
                        Filter::Vol => matches!(
                            opp.strategy_type.as_str(),
                            "vol_surface_anomaly" | "butterfly_spread"
                        ),
                        Filter::ShortPut => opp.strategy_type == "short_put_yield",
                        Filter::ShortCall => opp.strategy_type == "short_call_yield",
                        Filter::Portfolio => unreachable!(),
                    };
                    type_match && self.expiry_filter.matches(opp.expiry_timestamp)
                })
                .map(|(i, _)| i)
                .collect();
        }

        let leverage = self.leverage();
        let is_portfolio = self.filter == Filter::Portfolio;
        let opps: &[Opportunity] = if is_portfolio {
            &self.portfolios
        } else {
            &self.opportunities
        };
        match self.sort_by {
            SortBy::Profit => self.filtered.sort_by(|a, b| {
                opps[*b]
                    .expected_profit
                    .partial_cmp(&opps[*a].expected_profit)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            SortBy::Time => self
                .filtered
                .sort_by(|a, b| opps[*b].detected_at.cmp(&opps[*a].detected_at)),
            SortBy::Apy => self.filtered.sort_by(|a, b| {
                let apy_a = opps[*a]
                    .annualized_return_leveraged(leverage)
                    .unwrap_or(0.0);
                let apy_b = opps[*b]
                    .annualized_return_leveraged(leverage)
                    .unwrap_or(0.0);
                apy_b
                    .partial_cmp(&apy_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            SortBy::Expiry => self.filtered.sort_by(|a, b| {
                let exp_a = opps[*a].expiry_timestamp.unwrap_or(i64::MAX);
                let exp_b = opps[*b].expiry_timestamp.unwrap_or(i64::MAX);
                exp_a.cmp(&exp_b)
            }),
        }

        // Restore selection by key
        if let Some(ref key) = self.selected_key {
            if let Some(pos) = self
                .filtered
                .iter()
                .position(|&idx| Self::opp_key(&opps[idx]) == *key)
            {
                self.table_state.select(Some(pos));
            }
        }
    }
}

fn is_arb(strategy_type: &str) -> bool {
    matches!(
        strategy_type,
        "put_call_parity"
            | "box_spread"
            | "conversion"
            | "reversal"
            | "vertical_arb"
            | "butterfly_arb"
            | "calendar_arb"
    )
}

pub async fn run(
    mut opp_rx: mpsc::UnboundedReceiver<TuiEvent>,
    history_service: Option<ShortPutHistoryService>,
) -> Result<()> {
    terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    // Panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = terminal::disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    let result = run_inner(&mut opp_rx, history_service).await;

    // Restore terminal
    terminal::disable_raw_mode()?;
    execute!(std::io::stdout(), LeaveAlternateScreen)?;

    result
}

async fn run_inner(
    opp_rx: &mut mpsc::UnboundedReceiver<TuiEvent>,
    history_service: Option<ShortPutHistoryService>,
) -> Result<()> {
    let backend = CrosstermBackend::new(std::io::stdout());
    let mut terminal = Terminal::new(backend)?;
    let (history_req_tx, mut history_req_rx) = mpsc::unbounded_channel::<ShortPutHistoryRequest>();
    let (history_evt_tx, mut history_evt_rx) = mpsc::unbounded_channel::<TuiEvent>();
    let mut app = App::new(history_service.as_ref().map(|_| history_req_tx.clone()));
    let mut event_stream = EventStream::new();
    let mut tick = interval(Duration::from_millis(250));

    if let Some(history_service) = history_service {
        tokio::spawn(async move {
            while let Some(request) = history_req_rx.recv().await {
                let cache_key = request.cache_key.clone();
                let event = match history_service.load_history(&request).await {
                    Ok(history) => TuiEvent::ShortPutHistoryLoaded { cache_key, history },
                    Err(error) => TuiEvent::ShortPutHistoryFailed {
                        cache_key,
                        error: error.to_string(),
                    },
                };
                let _ = history_evt_tx.send(event);
            }
        });
    }

    loop {
        terminal.draw(|f| draw(f, &mut app))?;

        tokio::select! {
            event = event_stream.next() => {
                if let Some(Ok(CEvent::Key(key))) = event {
                    if key.kind == event::KeyEventKind::Press {
                        handle_key(&mut app, key);
                    }
                }
            }
            history_event = history_evt_rx.recv() => {
                match history_event {
                    Some(TuiEvent::ShortPutHistoryLoaded { cache_key, history }) => {
                        app.history_cache.insert(cache_key.clone(), history);
                        if app.pending_history_key.as_ref() == Some(&cache_key) {
                            app.pending_history_key = None;
                            app.detail_history_error = None;
                        }
                    }
                    Some(TuiEvent::ShortPutHistoryFailed { cache_key, error }) => {
                        if app.pending_history_key.as_ref() == Some(&cache_key) {
                            app.pending_history_key = None;
                            app.detail_history_error = Some(error);
                        }
                    }
                    _ => {}
                }
            }
            tui_event = opp_rx.recv() => {
                match tui_event {
                    Some(TuiEvent::Opportunity(opp)) => {
                        app.add_opportunity(opp);
                        app.recompute_portfolios();
                    }
                    Some(TuiEvent::Connected { instrument_count }) => {
                        app.connected = true;
                        app.instrument_count = instrument_count;
                    }
                    Some(TuiEvent::ShortPutHistoryLoaded { .. } | TuiEvent::ShortPutHistoryFailed { .. }) => {}
                    None => break,
                }
            }
            _ = tick.tick() => {
                app.update_filtered();
                // Clamp selection to valid range
                if let Some(sel) = app.table_state.selected() {
                    if sel >= app.filtered.len() {
                        app.table_state.select(if app.filtered.is_empty() { None } else { Some(app.filtered.len() - 1) });
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

fn handle_key(app: &mut App, key: event::KeyEvent) {
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.should_quit = true;
        return;
    }

    match app.view {
        View::List => match key.code {
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Up | KeyCode::Char('k') => {
                let selected = app.table_state.selected().unwrap_or(0);
                if selected > 0 {
                    app.table_state.select(Some(selected - 1));
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let selected = app.table_state.selected().unwrap_or(0);
                if selected < app.filtered.len().saturating_sub(1) {
                    app.table_state.select(Some(selected + 1));
                }
            }
            KeyCode::Enter => {
                if app.table_state.selected().is_some() && !app.filtered.is_empty() {
                    app.view = View::Detail;
                    app.request_history_for_current_selection();
                }
            }
            KeyCode::Char(c @ '0'..='9') => {
                app.filter = match c {
                    '1' => Filter::All,
                    '2' => Filter::Arbitrage,
                    '3' => Filter::Signal,
                    '4' => Filter::Pcp,
                    '5' => Filter::Spread,
                    '6' => Filter::ConvRev,
                    '7' => Filter::Calendar,
                    '8' => Filter::Vol,
                    '9' => Filter::ShortPut,
                    '0' => Filter::ShortCall,
                    _ => unreachable!(),
                };
                app.update_filtered();
                app.table_state.select(if app.filtered.is_empty() {
                    None
                } else {
                    Some(0)
                });
            }
            KeyCode::Char('l') => {
                app.leverage_idx = (app.leverage_idx + 1) % LEVERAGE_OPTIONS.len();
                app.recompute_portfolios();
                app.update_filtered();
            }
            KeyCode::Char('s') => {
                app.sort_by = match app.sort_by {
                    SortBy::Profit => SortBy::Apy,
                    SortBy::Apy => SortBy::Expiry,
                    SortBy::Expiry => SortBy::Time,
                    SortBy::Time => SortBy::Profit,
                };
                app.update_filtered();
            }
            KeyCode::Char('p') => {
                app.recompute_portfolios();
                app.filter = Filter::Portfolio;
                app.update_filtered();
                app.table_state.select(if app.filtered.is_empty() {
                    None
                } else {
                    Some(0)
                });
            }
            KeyCode::Char('e') => {
                app.expiry_filter = app.expiry_filter.next();
                app.update_filtered();
                app.table_state.select(if app.filtered.is_empty() {
                    None
                } else {
                    Some(0)
                });
            }
            _ => {}
        },
        View::Detail => match key.code {
            KeyCode::Esc | KeyCode::Backspace => app.view = View::List,
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('1') => {
                app.detail_resolution = HistoryResolution::OneHour;
                app.request_history_for_current_selection();
            }
            KeyCode::Char('4') => {
                app.detail_resolution = HistoryResolution::FourHours;
                app.request_history_for_current_selection();
            }
            _ => {}
        },
    }
}

fn draw(f: &mut Frame, app: &mut App) {
    match app.view {
        View::List => draw_list(f, app),
        View::Detail => draw_detail(f, app),
    }
}

fn draw_list(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(5),
        Constraint::Length(1),
    ])
    .split(f.area());

    // Header
    let leverage = app.leverage();
    let lev_str = if leverage > 1.0 {
        format!(" | {}x leverage", leverage as i32)
    } else {
        String::new()
    };
    let status = if app.connected {
        format!(
            "Connected | {} instruments | {} opportunities{}",
            app.instrument_count,
            app.filtered.len(),
            lev_str,
        )
    } else {
        "Connecting...".to_string()
    };
    let header = Paragraph::new(status)
        .block(
            Block::bordered()
                .title(" Deribit BTC Options Monitor ")
                .title_alignment(Alignment::Center),
        )
        .alignment(Alignment::Center);
    f.render_widget(header, chunks[0]);

    // Tabs - count only active (non-stale) opportunities
    let now = chrono::Utc::now().timestamp();
    let stale_threshold = 60;
    let active: Vec<&Opportunity> = app
        .opportunities
        .iter()
        .filter(|o| now - o.detected_at <= stale_threshold)
        .collect();
    let arb_count = active.iter().filter(|o| is_arb(&o.strategy_type)).count();
    let sig_count = active.len() - arb_count;
    let pcp_count = active
        .iter()
        .filter(|o| o.strategy_type == "put_call_parity")
        .count();
    let spread_count = active
        .iter()
        .filter(|o| {
            matches!(
                o.strategy_type.as_str(),
                "vertical_arb" | "butterfly_arb" | "box_spread"
            )
        })
        .count();
    let conv_count = active
        .iter()
        .filter(|o| matches!(o.strategy_type.as_str(), "conversion" | "reversal"))
        .count();
    let cal_count = active
        .iter()
        .filter(|o| matches!(o.strategy_type.as_str(), "calendar_arb" | "calendar_spread"))
        .count();
    let vol_count = active
        .iter()
        .filter(|o| {
            matches!(
                o.strategy_type.as_str(),
                "vol_surface_anomaly" | "butterfly_spread"
            )
        })
        .count();
    let short_put_count = active
        .iter()
        .filter(|o| o.strategy_type == "short_put_yield")
        .count();
    let short_call_count = active
        .iter()
        .filter(|o| o.strategy_type == "short_call_yield")
        .count();
    let tabs = Tabs::new(vec![
        format!("All [{}]", active.len()),
        format!("Arb [{}]", arb_count),
        format!("Sig [{}]", sig_count),
        format!("PCP [{}]", pcp_count),
        format!("Sprd [{}]", spread_count),
        format!("C/R [{}]", conv_count),
        format!("Cal [{}]", cal_count),
        format!("Vol [{}]", vol_count),
        format!("PUT [{}]", short_put_count),
        format!("CALL [{}]", short_call_count),
        format!("Port [{}]", app.portfolios.len()),
    ])
    .block(Block::bordered())
    .select(match app.filter {
        Filter::All => 0,
        Filter::Arbitrage => 1,
        Filter::Signal => 2,
        Filter::Pcp => 3,
        Filter::Spread => 4,
        Filter::ConvRev => 5,
        Filter::Calendar => 6,
        Filter::Vol => 7,
        Filter::ShortPut => 8,
        Filter::ShortCall => 9,
        Filter::Portfolio => 10,
    })
    .highlight_style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );
    f.render_widget(tabs, chunks[1]);

    // Table
    let header_row = Row::new(vec![
        Cell::from("Strategy"),
        Cell::from("Description"),
        Cell::from(format!(
            "Profit{}",
            if matches!(app.sort_by, SortBy::Profit) {
                " \u{2193}"
            } else {
                ""
            }
        )),
        Cell::from(format!(
            "APY{}",
            if matches!(app.sort_by, SortBy::Apy) {
                " \u{2193}"
            } else {
                ""
            }
        )),
        Cell::from("Risk"),
        Cell::from(format!(
            "Expiry{}",
            if matches!(app.sort_by, SortBy::Expiry) {
                " \u{2193}"
            } else {
                ""
            }
        )),
        Cell::from(format!(
            "Time{}",
            if matches!(app.sort_by, SortBy::Time) {
                " \u{2193}"
            } else {
                ""
            }
        )),
    ])
    .style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    let display = app.display_opps();
    let rows: Vec<Row> = app
        .filtered
        .iter()
        .map(|&idx| {
            let opp = &display[idx];
            let profit_str = if opp.expected_profit > 0.0 {
                format!("${:.2}", opp.expected_profit)
            } else {
                "\u{2014}".to_string()
            };
            let apy_str = match opp.annualized_return_leveraged(leverage) {
                Some(apy) => format!("{:.1}%", apy * 100.0),
                None => "\u{2014}".to_string(),
            };
            let time_str = chrono::DateTime::from_timestamp(opp.detected_at, 0)
                .map(|dt| dt.format("%H:%M:%S").to_string())
                .unwrap_or_default();
            let expiry_str = opp
                .expiry_timestamp
                .and_then(|ms| chrono::DateTime::from_timestamp_millis(ms))
                .map(|dt| {
                    let days = (dt - chrono::Utc::now()).num_days();
                    format!("{}d {}", days, dt.format("%m/%d"))
                })
                .unwrap_or_else(|| "\u{2014}".to_string());

            let risk_style = match opp.risk_level {
                RiskLevel::Low => Style::default().fg(Color::Green),
                RiskLevel::Medium => Style::default().fg(Color::Yellow),
                RiskLevel::High => Style::default().fg(Color::Red),
            };
            Row::new(vec![
                Cell::from(opp.strategy_type.clone()),
                Cell::from(truncate(&opp.description, 45)),
                Cell::from(profit_str),
                Cell::from(apy_str),
                Cell::from(opp.risk_level.to_string()).style(risk_style),
                Cell::from(expiry_str),
                Cell::from(time_str),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Min(20),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(header_row)
    .block(Block::bordered())
    .highlight_style(Style::default().bg(Color::DarkGray));

    f.render_stateful_widget(table, chunks[2], &mut app.table_state);

    // Footer
    let expiry_hint = if app.expiry_filter != ExpiryFilter::All {
        format!(" [{}]", app.expiry_filter.label())
    } else {
        String::new()
    };
    let footer = Paragraph::new(format!(
        " \u{2191}\u{2193}/jk Navigate | Enter Detail | 1-0 Filter | p Port | s Sort | e Expiry{} | l Leverage | q Quit",
        expiry_hint,
    ))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, chunks[3]);
}

fn draw_detail(f: &mut Frame, app: &mut App) {
    let opp = match app.selected_opportunity() {
        Some(o) => o,
        None => {
            app.view = View::List;
            return;
        }
    };

    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(10),
        Constraint::Min(5),
        Constraint::Length(8),
        Constraint::Length(1),
    ])
    .split(f.area());

    // Header
    let risk_color = match opp.risk_level {
        RiskLevel::Low => Color::Green,
        RiskLevel::Medium => Color::Yellow,
        RiskLevel::High => Color::Red,
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            opp.strategy_type.to_uppercase(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  |  Risk: "),
        Span::styled(opp.risk_level.to_string(), Style::default().fg(risk_color)),
    ]))
    .block(Block::bordered())
    .alignment(Alignment::Center);
    f.render_widget(header, chunks[0]);

    // Description
    let desc = Paragraph::new(format!("  {}", opp.description))
        .block(Block::bordered().title(" Description "))
        .wrap(Wrap { trim: false });
    f.render_widget(desc, chunks[1]);

    draw_short_put_history_block(f, chunks[2], app, &opp);

    // Legs table
    if !opp.legs.is_empty() {
        let leg_header = Row::new(vec!["Step", "Action", "Instrument", "Price", "Unit", "Qty"])
            .style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            );

        let leg_rows: Vec<Row> = opp
            .legs
            .iter()
            .map(|leg| {
                let action_style = match leg.action {
                    Action::Buy => Style::default().fg(Color::Green),
                    Action::Sell => Style::default().fg(Color::Red),
                };
                Row::new(vec![
                    Cell::from(format!("  {}", leg.step)),
                    Cell::from(leg.action.to_string()).style(action_style),
                    Cell::from(leg.instrument.clone()),
                    Cell::from(format!("{:.6}", leg.price)),
                    Cell::from(leg.price_unit.to_string()),
                    Cell::from(format!("{:.1}", leg.amount)),
                ])
            })
            .collect();

        let leg_table = Table::new(
            leg_rows,
            [
                Constraint::Length(6),
                Constraint::Length(6),
                Constraint::Min(25),
                Constraint::Length(12),
                Constraint::Length(5),
                Constraint::Length(6),
            ],
        )
        .header(leg_header)
        .block(Block::bordered().title(" Execution Steps "));

        f.render_widget(leg_table, chunks[3]);
    }

    // Profit info
    let leverage = app.leverage();
    let leveraged_cost = opp.leveraged_cost(leverage);
    let mut info_lines = Vec::new();
    if opp.total_cost != 0.0 {
        let cost_label = if leverage > 1.0 {
            format!(
                "  Capital ({}x):    ${:.2}  (full: ${:.2})",
                leverage as i32, leveraged_cost, opp.total_cost
            )
        } else {
            format!("  Total Cost:      ${:.2}", opp.total_cost)
        };
        info_lines.push(Line::from(cost_label));
    }
    if opp.expected_profit > 0.0 {
        info_lines.push(Line::from(vec![
            Span::raw("  Expected Profit: "),
            Span::styled(
                format!("${:.2}", opp.expected_profit),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
        if leveraged_cost > 1.0 {
            let roi = (opp.expected_profit / leveraged_cost) * 100.0;
            info_lines.push(Line::from(format!("  ROI:             {:.2}%", roi)));
        }
        if let Some(apy) = opp.annualized_return_leveraged(leverage) {
            let apy_label = if leverage > 1.0 {
                format!("  APY ({}x):        ", leverage as i32)
            } else {
                "  Annualized (APY): ".to_string()
            };
            info_lines.push(Line::from(vec![
                Span::raw(apy_label),
                Span::styled(
                    format!("{:.1}%", apy * 100.0),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));
        }
    }
    let time_str = chrono::DateTime::from_timestamp(opp.detected_at, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "unknown".to_string());
    info_lines.push(Line::from(format!("  Detected:        {}", time_str)));
    if let Some(expiry_ms) = opp.expiry_timestamp {
        let expiry_str = chrono::DateTime::from_timestamp_millis(expiry_ms)
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let days_left = (expiry_ms - chrono::Utc::now().timestamp_millis()) as f64 / 86_400_000.0;
        info_lines.push(Line::from(format!(
            "  Expiry:          {} ({:.0} days)",
            expiry_str, days_left
        )));
    }
    info_lines.push(Line::from(format!(
        "  Instruments:     {}",
        opp.instruments.join(", ")
    )));

    let profit_block = Paragraph::new(info_lines).block(Block::bordered().title(" Details "));
    f.render_widget(profit_block, chunks[4]);

    // Footer
    let footer = Paragraph::new(" 1 1h | 4 4h | Esc Back | q Quit")
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, chunks[5]);
}

fn draw_short_put_history_block(f: &mut Frame, area: Rect, app: &App, opp: &Opportunity) {
    if opp.strategy_type != "short_put_yield" && opp.strategy_type != "short_call_yield" {
        let widget =
            Paragraph::new(" History chart is only available for short put/call opportunities.")
                .block(Block::bordered().title(" Approx APY History "));
        f.render_widget(widget, area);
        return;
    }

    let title = format!(" Approx APY History ({}) ", app.detail_resolution.label());

    if let Some(error) = &app.detail_history_error {
        let widget = Paragraph::new(format!(" {}\n\n Press 1 or 5 to retry.", error))
            .block(Block::bordered().title(title));
        f.render_widget(widget, area);
        return;
    }

    if let Some(pending) = &app.pending_history_key {
        if pending == &App::history_cache_key(opp, app.detail_resolution) {
            let widget = Paragraph::new(format!(
                " Loading {} approx APY history...\n\n This fetches only after you open detail.",
                app.detail_resolution.label()
            ))
            .block(Block::bordered().title(title));
            f.render_widget(widget, area);
            return;
        }
    }

    let Some(history) = app.current_detail_history(opp) else {
        let widget = Paragraph::new(" Press 1 or 5 to load approximate APY history.")
            .block(Block::bordered().title(title));
        f.render_widget(widget, area);
        return;
    };

    if history.points.is_empty() {
        let widget = Paragraph::new(format!(
            " {}\n\n No qualifying trades in the selected window.",
            history.status
        ))
        .block(Block::bordered().title(title));
        f.render_widget(widget, area);
        return;
    }

    let data: Vec<(f64, f64)> = history
        .points
        .iter()
        .enumerate()
        .map(|(idx, point)| (idx as f64, point.annualized_return * 100.0))
        .collect();
    let min_y = data.iter().map(|(_, y)| *y).fold(f64::INFINITY, f64::min);
    let max_y = data
        .iter()
        .map(|(_, y)| *y)
        .fold(f64::NEG_INFINITY, f64::max);
    let (min_y, max_y) = if (max_y - min_y).abs() < 0.01 {
        (min_y - 1.0, max_y + 1.0)
    } else {
        (min_y.floor(), max_y.ceil())
    };

    let start_label = history
        .points
        .first()
        .and_then(|point| chrono::DateTime::from_timestamp_millis(point.bucket_start_ms))
        .map(|dt| dt.format("%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "start".to_string());
    let end_label = history
        .points
        .last()
        .and_then(|point| chrono::DateTime::from_timestamp_millis(point.bucket_start_ms))
        .map(|dt| dt.format("%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "end".to_string());
    let mid_label = history
        .points
        .get(history.points.len() / 2)
        .and_then(|point| chrono::DateTime::from_timestamp_millis(point.bucket_start_ms))
        .map(|dt| dt.format("%m-%d %H:%M").to_string())
        .unwrap_or_else(|| end_label.clone());

    let inner = Layout::vertical([Constraint::Length(3), Constraint::Min(0)]).split(area);
    let summary = Paragraph::new(format!(
        " {} | range {} -> {} | {} pts",
        history.status,
        start_label,
        end_label,
        history.points.len()
    ))
    .block(Block::bordered().title(title.clone()));
    f.render_widget(summary, inner[0]);

    let chart = Chart::new(vec![Dataset::default()
        .name("Approx APY")
        .marker(Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(Color::Cyan))
        .data(&data)])
    .block(Block::bordered())
    .x_axis(
        Axis::default()
            .bounds([0.0, (data.len().saturating_sub(1)) as f64])
            .labels(vec![
                Line::from(start_label),
                Line::from(mid_label),
                Line::from(end_label),
            ]),
    )
    .y_axis(Axis::default().bounds([min_y, max_y]).labels(vec![
        Line::from(format!("{:.1}%", min_y)),
        Line::from(format!("{:.1}%", ((min_y + max_y) / 2.0))),
        Line::from(format!("{:.1}%", max_y)),
    ]))
    .legend_position(None);
    f.render_widget(chart, inner[1]);
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{}\u{2026}", truncated)
    } else {
        s.to_string()
    }
}
