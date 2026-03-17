use std::collections::HashMap;

use anyhow::Result;
use crossterm::event::{self, Event as CEvent, EventStream, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use futures_util::StreamExt;
use ratatui::prelude::*;
use ratatui::widgets::*;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

use crate::analysis::opportunity::{Action, Opportunity, RiskLevel};
use crate::analysis::portfolio::PortfolioOptimizer;

pub enum TuiEvent {
    Opportunity(Opportunity),
    Connected { instrument_count: usize },
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
    Portfolio,
}

#[derive(PartialEq, Clone, Copy)]
enum SortBy {
    Profit,
    Time,
    Apy,
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
}

impl App {
    fn new() -> Self {
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
                    match self.filter {
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
                        Filter::Portfolio => unreachable!(),
                    }
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

pub async fn run(mut opp_rx: mpsc::UnboundedReceiver<TuiEvent>) -> Result<()> {
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

    let result = run_inner(&mut opp_rx).await;

    // Restore terminal
    terminal::disable_raw_mode()?;
    execute!(std::io::stdout(), LeaveAlternateScreen)?;

    result
}

async fn run_inner(opp_rx: &mut mpsc::UnboundedReceiver<TuiEvent>) -> Result<()> {
    let backend = CrosstermBackend::new(std::io::stdout());
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::new();
    let mut event_stream = EventStream::new();
    let mut tick = interval(Duration::from_millis(250));

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
                    '0' => {
                        app.recompute_portfolios();
                        Filter::Portfolio
                    }
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
                    SortBy::Apy => SortBy::Time,
                    SortBy::Time => SortBy::Profit,
                };
                app.update_filtered();
            }
            _ => {}
        },
        View::Detail => match key.code {
            KeyCode::Esc | KeyCode::Backspace => app.view = View::List,
            KeyCode::Char('q') => app.should_quit = true,
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
        Filter::Portfolio => 9,
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
                Cell::from(time_str),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(16),
            Constraint::Min(25),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(header_row)
    .block(Block::bordered())
    .highlight_style(Style::default().bg(Color::DarkGray));

    f.render_stateful_widget(table, chunks[2], &mut app.table_state);

    // Footer
    let footer = Paragraph::new(
        " \u{2191}\u{2193}/jk Navigate | Enter Detail | 1-9/0 Filter | s Sort | l Leverage | q Quit",
    )
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, chunks[3]);
}

fn draw_detail(f: &mut Frame, app: &mut App) {
    let opp = match app
        .table_state
        .selected()
        .and_then(|i| app.filtered.get(i))
        .map(|&idx| app.display_opps()[idx].clone())
    {
        Some(o) => o,
        None => {
            app.view = View::List;
            return;
        }
    };

    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(4),
        Constraint::Min(5),
        Constraint::Length(9),
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

        f.render_widget(leg_table, chunks[2]);
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
    f.render_widget(profit_block, chunks[3]);

    // Footer
    let footer = Paragraph::new(" Esc Back | q Quit").style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, chunks[4]);
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{}\u{2026}", truncated)
    } else {
        s.to_string()
    }
}
