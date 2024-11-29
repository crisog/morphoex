use alloy_primitives::{address, Address};
use alloy_sol_types::{sol, SolEventInterface};
use futures::{Future, FutureExt, TryStreamExt};
use reth_execution_types::Chain;
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::BlockBody;
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_primitives::{Log, SealedBlockWithSenders, TransactionSigned};
use reth_tracing::tracing::info;
use rusqlite::Connection;

sol!(Morpho, "morpho_abi.json");
use Morpho::MorphoEvents;

const MORPHO_ADDRESS: Address = address!("BBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb");

async fn init<Node: FullNodeComponents>(
    ctx: ExExContext<Node>,
    mut connection: Connection,
) -> eyre::Result<impl Future<Output = eyre::Result<()>>> {
    create_tables(&mut connection)?;
    Ok(morpho_monitor(ctx, connection))
}

fn create_tables(connection: &mut Connection) -> rusqlite::Result<()> {
    connection.execute(
        r#"
        CREATE TABLE IF NOT EXISTS markets (
            id TEXT PRIMARY KEY,
            loan_token TEXT NOT NULL,
            collateral_token TEXT NOT NULL,
            oracle TEXT NOT NULL,
            irm TEXT NOT NULL,
            lltv TEXT NOT NULL,
            total_borrow_assets TEXT NOT NULL,
            total_borrow_shares TEXT NOT NULL,
            last_update INTEGER NOT NULL
        );"#,
        (),
    )?;

    connection.execute(
        r#"
        CREATE TABLE IF NOT EXISTS positions (
            market_id TEXT NOT NULL,
            borrower TEXT NOT NULL,
            borrow_shares TEXT NOT NULL,
            collateral TEXT NOT NULL,
            last_updated INTEGER NOT NULL,
            PRIMARY KEY (market_id, borrower)
        );"#,
        (),
    )?;

    connection.execute(
        r#"
        CREATE TABLE IF NOT EXISTS oracle_prices (
            oracle_address TEXT NOT NULL,
            price TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            PRIMARY KEY (oracle_address, block_number)
        );"#,
        (),
    )?;

    connection.execute(
        r#"
        CREATE TABLE IF NOT EXISTS market_states (
            market_id TEXT NOT NULL,
            total_borrow_assets TEXT NOT NULL,
            total_borrow_shares TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            PRIMARY KEY (market_id, block_number, log_index)
        );"#,
        (),
    )?;

    Ok(())
}

async fn morpho_monitor<Node: FullNodeComponents>(
    mut ctx: ExExContext<Node>,
    connection: Connection,
) -> eyre::Result<()> {
    while let Some(notification) = ctx.notifications.try_next().await? {
        // Handle chain reorgs/reverts
        if let Some(reverted_chain) = notification.reverted_chain() {
            info!(chain_range = ?reverted_chain.range(), "Reverting chain");

            let start_block = *reverted_chain.range().start();

            connection.execute(
                "DELETE FROM positions WHERE last_updated >= ?",
                [start_block],
            )?;

            connection.execute(
                "DELETE FROM market_states WHERE block_number >= ?",
                [start_block],
            )?;

            connection.execute(
                "DELETE FROM oracle_prices WHERE block_number >= ?",
                [start_block],
            )?;
        }

        if let Some(committed_chain) = notification.committed_chain() {
            info!(chain_range = ?committed_chain.range(), "Processing new chain");

            for (block, _tx, (log, log_idx), event) in decode_chain_events(&committed_chain) {
                match event {
                    MorphoEvents::CreateMarket(e) => {
                        connection.execute(
                            "INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                e.id.to_string(),
                                e.marketParams.loanToken.to_string(),
                                e.marketParams.collateralToken.to_string(),
                                e.marketParams.oracle.to_string(),
                                e.marketParams.irm.to_string(),
                                e.marketParams.lltv.to_string(),
                                "0",
                                "0",
                                block.timestamp,
                            ),
                        )?;
                    }
                    MorphoEvents::SupplyCollateral(e) => {
                        connection.execute(
                            r#"
                            INSERT INTO positions (market_id, borrower, borrow_shares, collateral, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(market_id, borrower) DO UPDATE SET
                            collateral = CAST(collateral AS INTEGER) + ?,
                            last_updated = ?
                            "#,
                            (
                                e.id.to_string(),
                                e.onBehalf.to_string(),
                                "0",
                                e.assets.to_string(),
                                block.timestamp,
                                e.assets.to_string(),
                                block.timestamp,
                            ),
                        )?;
                    }
                    MorphoEvents::Borrow(e) => {
                        connection.execute(
                            r#"
                            INSERT INTO positions (market_id, borrower, borrow_shares, collateral, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(market_id, borrower) DO UPDATE SET
                            borrow_shares = CAST(borrow_shares AS INTEGER) + ?,
                            last_updated = ?
                            "#,
                            (
                                e.id.to_string(),
                                e.onBehalf.to_string(),
                                e.shares.to_string(),
                                "0",
                                block.timestamp,
                                e.shares.to_string(),
                                block.timestamp,
                            ),
                        )?;

                        connection.execute(
                            "INSERT INTO market_states VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                e.id.to_string(),
                                e.assets.to_string(),
                                e.shares.to_string(),
                                log_idx,
                                block.number,
                                block.timestamp,
                            ),
                        )?;
                    }
                    MorphoEvents::Repay(e) => {
                        connection.execute(
                            r#"
                            INSERT INTO positions (market_id, borrower, borrow_shares, collateral, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(market_id, borrower) DO UPDATE SET
                            borrow_shares = CAST(borrow_shares AS INTEGER) - ?,
                            last_updated = ?
                            "#,
                            (
                                e.id.to_string(),
                                e.onBehalf.to_string(),
                                "0",
                                "0",
                                block.timestamp,
                                e.shares.to_string(),
                                block.timestamp,
                            ),
                        )?;
                    }
                    MorphoEvents::WithdrawCollateral(e) => {
                        connection.execute(
                            r#"
                            INSERT INTO positions (market_id, borrower, borrow_shares, collateral, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(market_id, borrower) DO UPDATE SET
                            collateral = CAST(collateral AS INTEGER) - ?,
                            last_updated = ?
                            "#,
                            (
                                e.id.to_string(),
                                e.onBehalf.to_string(),
                                "0",
                                "0",
                                block.timestamp,
                                e.assets.to_string(),
                                block.timestamp,
                            ),
                        )?;
                    }
                    MorphoEvents::AccrueInterest(e) => {
                        connection.execute(
                            "INSERT INTO market_states VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                e.id.to_string(),
                                e.interest.to_string(),
                                "0",
                                log_idx,
                                block.number,
                                block.timestamp,
                            ),
                        )?;
                    }
                    MorphoEvents::Liquidate(e) => {
                        connection.execute(
                            r#"
                            INSERT INTO positions (market_id, borrower, borrow_shares, collateral, last_updated)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(market_id, borrower) DO UPDATE SET
                            borrow_shares = CAST(borrow_shares AS INTEGER) - ? - ?,
                            collateral = CAST(collateral AS INTEGER) - ?,
                            last_updated = ?
                            "#,
                            (
                                e.id.to_string(),
                                e.borrower.to_string(),
                                "0",
                                "0",
                                block.timestamp,
                                e.repaidShares.to_string(),
                                e.badDebtShares.to_string(),
                                e.seizedAssets.to_string(),
                                block.timestamp,
                            ),
                        )?;
                    }
                    _ => {
                        continue;
                    }
                }
            }

            check_positions(&committed_chain, &connection).await?;

            ctx.events
                .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }
    }

    Ok(())
}

fn decode_chain_events(
    chain: &Chain,
) -> impl Iterator<
    Item = (
        &SealedBlockWithSenders,
        &TransactionSigned,
        (&Log, usize),
        MorphoEvents,
    ),
> {
    chain
        .blocks_and_receipts()
        .flat_map(|(block, receipts)| {
            block
                .body
                .transactions()
                .into_iter()
                .zip(receipts.iter().flatten())
                .map(move |(tx, receipt)| (block, tx, receipt))
        })
        .flat_map(|(block, tx, receipt)| {
            receipt
                .logs
                .iter()
                .enumerate()
                .filter(|(_, log)| log.address == MORPHO_ADDRESS)
                .map(move |(idx, log)| (block, tx, (log, idx)))
        })
        .filter_map(|(block, tx, (log, idx))| {
            MorphoEvents::decode_raw_log(log.topics(), &log.data.data, true)
                .ok()
                .map(|event| (block, tx, (log, idx), event))
        })
}

const WARNING_THRESHOLD: f64 = 0.95;
const HIGH_RISK_THRESHOLD: f64 = 0.98;
const WAD: u128 = 1_000_000_000_000_000_000;
const ORACLE_PRICE_SCALE: u128 = 1_000_000_000_000_000_000_000_000_000_000_000_000;

fn calculate_position_metrics(
    borrow_shares: String,
    total_borrow_assets: String,
    total_borrow_shares: String,
    collateral: String,
    price: String,
    lltv: String,
) -> rusqlite::Result<(bool, f64)> {
    let borrow_shares = borrow_shares
        .parse::<u128>()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let total_borrow_assets = total_borrow_assets
        .parse::<u128>()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let total_borrow_shares = total_borrow_shares
        .parse::<u128>()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let collateral = collateral
        .parse::<u128>()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let price = price
        .parse::<u128>()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let lltv = lltv
        .parse::<u128>()
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

    let borrowed = (borrow_shares as f64 * total_borrow_assets as f64) / total_borrow_shares as f64;
    let collateral_value = (collateral as f64 * price as f64) / ORACLE_PRICE_SCALE as f64;
    let max_borrow = (collateral_value * lltv as f64) / WAD as f64;

    Ok((max_borrow >= borrowed, borrowed / collateral_value))
}

async fn check_positions(chain: &Chain, connection: &Connection) -> rusqlite::Result<()> {
    let mut stmt = connection.prepare(
        r#"
        SELECT 
            p.market_id,
            p.borrower,
            p.borrow_shares,
            p.collateral,
            m.total_borrow_assets,
            m.total_borrow_shares,
            m.lltv,
            m.oracle,
            op.price
        FROM positions p
        INNER JOIN markets m ON p.market_id = m.id
        INNER JOIN oracle_prices op ON m.oracle = op.oracle_address
        WHERE p.borrow_shares > 0 
        AND p.collateral > 0
        AND op.block_number = ?
        "#,
    )?;

    for block in chain.blocks() {
        let positions = stmt.query_map([block.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;

        for position in positions {
            let (
                market_id,
                borrower,
                borrow_shares,
                collateral,
                total_borrow_assets,
                total_borrow_shares,
                lltv,
                _oracle,
                price,
            ) = position?;

            if let Ok((is_healthy, ltv_ratio)) = calculate_position_metrics(
                borrow_shares,
                total_borrow_assets,
                total_borrow_shares,
                collateral,
                price,
                lltv,
            ) {
                if !is_healthy {
                    info!(
                        market_id = %market_id,
                        borrower = %borrower,
                        ltv = %ltv_ratio,
                        "🚨 LIQUIDATABLE POSITION"
                    );
                } else if ltv_ratio >= HIGH_RISK_THRESHOLD {
                    info!(
                        market_id = %market_id,
                        borrower = %borrower,
                        ltv = %ltv_ratio,
                        "⚠️ HIGH RISK POSITION"
                    );
                } else if ltv_ratio >= WARNING_THRESHOLD {
                    info!(
                        market_id = %market_id,
                        borrower = %borrower,
                        ltv = %ltv_ratio,
                        "RISK WARNING"
                    );
                }
            }
        }
    }

    Ok(())
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("morpho-monitor", move |ctx| {
                tokio::task::spawn_blocking(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        let connection = Connection::open("morpho.db")?;
                        init(ctx, connection).await
                    })
                })
                .map(|result| result.map_err(Into::into).and_then(|result| result))
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
