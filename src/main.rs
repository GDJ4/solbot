use futures::{SinkExt, StreamExt};
use lazy_static::lazy_static;
use reqwest::Client;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use anyhow::{anyhow, Result};
use base58::FromBase58;
use byteorder::{LittleEndian, ReadBytesExt};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash, // для работы с типом blockhash
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_program,
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use spl_token;
use std::io::Cursor;
use std::str::FromStr;
use std::time::Instant;
use std::fs::OpenOptions;
use fern::Dispatch;
use log::{info, error};
use chrono::Local;

#[macro_use]
extern crate lazy_static;

// ====================================================================
// Параметры мониторинга и покупки

const HELIUS_HTTP_URL: &str =
    "https://mainnet.helius-rpc.com/?api-key=599e8cf5-326a-45f5-bf87-28e11694b53c";
const HELIUS_WS_URL: &str =
    "wss://mainnet.helius-rpc.com/?api-key=599e8cf5-326a-45f5-bf87-28e11694b53c";
const OWNER_PUBKEY: &str = "AJJUdocriJ3yTV6zqU4S8h5EpPeQBMsscru2Hysy6yHD";

const BUY_AMOUNT: f64 = 0.001;
const BUY_SLIPPAGE: f64 = 999.0;
const MAX_RETRIES: usize = 5;

const RPC_ENDPOINT: &str =
    "https://nd-515-068-787.p2pify.com/3b5330af059bfe5e13b03acadfe42ee9";

const RPC_ENDPOINT1: &str =
    "https://mainnet.helius-rpc.com/?api-key=599e8cf5-326a-45f5-bf87-28e11694b53c";
// ====================================================================
// Константы для расчётов

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const TOKEN_DECIMALS: u32 = 6;
const EXPECTED_DISCRIMINATOR: [u8; 8] = 6966180631402821399u64.to_le_bytes();

// ====================================================================
// Статические адреса программ (lazy_static)

lazy_static! {
    static ref PUMP_PROGRAM: Pubkey =
        Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();
    static ref PUMP_GLOBAL: Pubkey =
        Pubkey::from_str("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf").unwrap();
    static ref PUMP_EVENT_AUTHORITY: Pubkey =
        Pubkey::from_str("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1").unwrap();
    static ref PUMP_FEE: Pubkey =
        Pubkey::from_str("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM").unwrap();
    static ref SYSTEM_PROGRAM: Pubkey =
        Pubkey::from_str("11111111111111111111111111111111").unwrap();
    static ref SYSTEM_TOKEN_PROGRAM: Pubkey =
        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    static ref SYSTEM_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM: Pubkey =
        Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").unwrap();
    static ref SYSTEM_RENT: Pubkey =
        Pubkey::from_str("SysvarRent111111111111111111111111111111111").unwrap();
    static ref SOL: Pubkey =
        Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
    static ref HTTP_CLIENT: Client = Client::new();
}

// ====================================================================
// Структуры для десериализации ответа метода getLatestBlockhash

#[derive(Debug, serde::Deserialize)]
struct GetLatestBlockhashResponse {
    jsonrpc: String,
    result: BlockhashResult,
    id: u64,
}

#[derive(Debug, serde::Deserialize)]
struct BlockhashResult {
    context: serde_json::Value, // Можно заменить на конкретную структуру, если нужно
    value: BlockhashValue,
}
#[derive(Debug, serde::Deserialize)]
struct BlockhashValue {
    blockhash: String,
    lastValidBlockHeight: u64,
}

// ====================================================================
// Функция для получения последнего blockhash через RPC (HTTP)

async fn fetch_latest_blockhash(rpc_url: &str) -> Result<Hash> {
    let client = Client::new();
    let request_body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": []
    });
    let response = client.post(rpc_url)
        .json(&request_body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Ошибка запроса: {} - {}", status, text));
    }

    let resp_json: GetLatestBlockhashResponse = response.json().await?;
    let blockhash_str = resp_json.result.value.blockhash;
    let blockhash = Hash::from_str(&blockhash_str)
        .map_err(|e| anyhow!("Ошибка преобразования blockhash: {:?}", e))?;
    Ok(blockhash)
}

// ====================================================================
// Функция, которая каждые 2 с обновляет разделяемую переменную с последним blockhash

async fn poll_latest_blockhash(rpc_url: &str, latest_blockhash: Arc<Mutex<Hash>>) {
    loop {
        match fetch_latest_blockhash(rpc_url).await {
            Ok(new_hash) => {
                let mut lock = latest_blockhash.lock().await;
                *lock = new_hash;
                // При необходимости можно раскомментировать:
                // info!("Обновлён blockhash: {}", new_hash);
            }
            Err(e) => {
                error!("Ошибка получения blockhash: {:?}", e);
            }
        }
        sleep(Duration::from_millis(2000)).await;
    }
}

// ====================================================================
// Функция получения списка токеновых аккаунтов (mint'ов) для указанного владельца

async fn get_token_mints(http_url: &str, owner: &str) -> Result<Vec<String>> {
    let func_start = Instant::now();
    info!("[{} ms] [get_token_mints] Функция запущена.", func_start.elapsed().as_millis());

    // Формирование payload
    let payload_start = Instant::now();
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            owner,
            { "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" },
            { "encoding": "jsonParsed", "commitment": "processed" }
        ]
    });
    info!("[{} ms] [get_token_mints] Payload сформирован ({} ms).", func_start.elapsed().as_millis(), payload_start.elapsed().as_millis());

    // Отправка запроса
    let client = &*HTTP_CLIENT;
    let req_send_start = Instant::now();
    let response = client.post(http_url).json(&payload).send().await?;
    info!("[{} ms] [get_token_mints] HTTP-запрос отправлен ({} ms).", func_start.elapsed().as_millis(), req_send_start.elapsed().as_millis());

    // Получение и разбор ответа
    let parse_start = Instant::now();
    let resp_json = response.json::<serde_json::Value>().await?;
    info!("[{} ms] [get_token_mints] Ответ получен и распарсен ({} ms).", func_start.elapsed().as_millis(), parse_start.elapsed().as_millis());

    // Обработка JSON-ответа
    let mut mints = Vec::new();
    if let Some(result) = resp_json.get("result") {
        let result_found = Instant::now();
        info!("[{} ms] [get_token_mints] Ключ 'result' найден ({} ms).", func_start.elapsed().as_millis(), result_found.elapsed().as_millis());
        if let Some(value) = result.get("value") {
            let value_found = Instant::now();
            info!("[{} ms] [get_token_mints] Ключ 'value' найден ({} ms).", func_start.elapsed().as_millis(), value_found.elapsed().as_millis());
            if let Some(array) = value.as_array() {
                let array_start = Instant::now();
                info!("[{} ms] [get_token_mints] 'value' преобразован в массив ({} ms).", func_start.elapsed().as_millis(), array_start.elapsed().as_millis());
                for (i, item) in array.iter().enumerate() {
                    let iter_start = Instant::now();
                    info!("[{} ms] [get_token_mints] Начало обработки элемента {}.", func_start.elapsed().as_millis(), i);
                    if let Some(account) = item.get("account") {
                        info!("[{} ms] [get_token_mints] Элемент {}: найден ключ 'account'.", func_start.elapsed().as_millis(), i);
                        if let Some(data) = account.get("data") {
                            info!("[{} ms] [get_token_mints] Элемент {}: найден ключ 'data'.", func_start.elapsed().as_millis(), i);
                            if let Some(parsed) = data.get("parsed") {
                                info!("[{} ms] [get_token_mints] Элемент {}: найден ключ 'parsed'.", func_start.elapsed().as_millis(), i);
                                if let Some(info_obj) = parsed.get("info") {
                                    info!("[{} ms] [get_token_mints] Элемент {}: найден ключ 'info'.", func_start.elapsed().as_millis(), i);
                                    if let Some(mint) = info_obj.get("mint").and_then(|m| m.as_str()) {
                                        info!("[{} ms] [get_token_mints] Элемент {}: найден mint: {}.", func_start.elapsed().as_millis(), i, mint);
                                        mints.push(mint.to_string());
                                    } else {
                                        info!("[{} ms] [get_token_mints] Элемент {}: ключ 'mint' отсутствует.", func_start.elapsed().as_millis(), i);
                                    }
                                } else {
                                    info!("[{} ms] [get_token_mints] Элемент {}: ключ 'info' отсутствует.", func_start.elapsed().as_millis(), i);
                                }
                            } else {
                                info!("[{} ms] [get_token_mints] Элемент {}: ключ 'parsed' отсутствует.", func_start.elapsed().as_millis(), i);
                            }
                        } else {
                            info!("[{} ms] [get_token_mints] Элемент {}: ключ 'data' отсутствует.", func_start.elapsed().as_millis(), i);
                        }
                    } else {
                        info!("[{} ms] [get_token_mints] Элемент {}: ключ 'account' отсутствует.", func_start.elapsed().as_millis(), i);
                    }
                    info!("[{} ms] [get_token_mints] Обработка элемента {} завершена ({} ms).", func_start.elapsed().as_millis(), i, iter_start.elapsed().as_millis());
                }
                info!("[{} ms] [get_token_mints] Итерация по массиву завершена ({} ms, найдено {} mint-ов).", func_start.elapsed().as_millis(), array_start.elapsed().as_millis(), mints.len());
            } else {
                info!("[{} ms] [get_token_mints] Ключ 'value' не является массивом.", func_start.elapsed().as_millis());
            }
        } else {
            info!("[{} ms] [get_token_mints] Ключ 'value' не найден в 'result'.", func_start.elapsed().as_millis());
        }
    } else {
        info!("[{} ms] [get_token_mints] Ключ 'result' отсутствует в ответе.", func_start.elapsed().as_millis());
    }
    info!("[{} ms] [get_token_mints] Функция завершена.", func_start.elapsed().as_millis());
    Ok(mints)
}

// ====================================================================
// Функции для работы с bonding curve
fn get_bonding_curve_address(mint: &Pubkey, program_id: &Pubkey) -> (Pubkey, u8) {
    let start = Instant::now();
    let res = Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], program_id);
    info!("[{} ms] [get_bonding_curve_address] Вычислен адрес bonding curve ({} ms).", start.elapsed().as_millis(), start.elapsed().as_millis());
    res
}

fn find_associated_bonding_curve(mint: &Pubkey, bonding_curve: &Pubkey) -> Pubkey {
    let start = Instant::now();
    let token_program_id = *SYSTEM_TOKEN_PROGRAM;
    let ata_program_id = *SYSTEM_ASSOCIATED_TOKEN_ACCOUNT_PROGRAM;
    let (derived_address, _bump_seed) = Pubkey::find_program_address(
        &[bonding_curve.as_ref(), token_program_id.as_ref(), mint.as_ref()],
        &ata_program_id,
    );
    info!("[{} ms] [find_associated_bonding_curve] ATA адрес найден ({} ms).", start.elapsed().as_millis(), start.elapsed().as_millis());
    derived_address
}

#[derive(Debug)]
struct BondingCurveState {
    virtual_token_reserves: u64,
    virtual_sol_reserves: u64,
    real_token_reserves: u64,
    real_sol_reserves: u64,
    token_total_supply: u64,
    complete: bool,
}

impl BondingCurveState {
    fn from_account_data(data: &[u8]) -> Result<Self> {
        let start = Instant::now();
        info!("[{} ms] [from_account_data] Начало декодирования аккаунта.", start.elapsed().as_millis());
        if data.len() < 8 + 41 {
            return Err(anyhow!("Длина данных меньше ожидаемой"));
        }
        info!("[{} ms] [from_account_data] Проверка длины прошла успешно.", start.elapsed().as_millis());
        if data[..8] != EXPECTED_DISCRIMINATOR {
            return Err(anyhow!("Неверный дискриминатор"));
        }
        info!("[{} ms] [from_account_data] Дискриминатор корректный.", start.elapsed().as_millis());
        let mut rdr = Cursor::new(&data[8..]);
        let vt_start = Instant::now();
        let virtual_token_reserves = rdr.read_u64::<LittleEndian>()?;
        info!("[{} ms] [from_account_data] virtual_token_reserves: {} ({} ms).", start.elapsed().as_millis(), virtual_token_reserves, vt_start.elapsed().as_millis());
        let vs_start = Instant::now();
        let virtual_sol_reserves = rdr.read_u64::<LittleEndian>()?;
        info!("[{} ms] [from_account_data] virtual_sol_reserves: {} ({} ms).", start.elapsed().as_millis(), virtual_sol_reserves, vs_start.elapsed().as_millis());
        let rt_start = Instant::now();
        let real_token_reserves = rdr.read_u64::<LittleEndian>()?;
        info!("[{} ms] [from_account_data] real_token_reserves: {} ({} ms).", start.elapsed().as_millis(), real_token_reserves, rt_start.elapsed().as_millis());
        let rs_start = Instant::now();
        let real_sol_reserves = rdr.read_u64::<LittleEndian>()?;
        info!("[{} ms] [from_account_data] real_sol_reserves: {} ({} ms).", start.elapsed().as_millis(), real_sol_reserves, rs_start.elapsed().as_millis());
        let ts_start = Instant::now();
        let token_total_supply = rdr.read_u64::<LittleEndian>()?;
        info!("[{} ms] [from_account_data] token_total_supply: {} ({} ms).", start.elapsed().as_millis(), token_total_supply, ts_start.elapsed().as_millis());
        let complete_start = Instant::now();
        let complete = rdr.read_u8()? != 0;
        info!("[{} ms] [from_account_data] complete: {} ({} ms).", start.elapsed().as_millis(), complete, complete_start.elapsed().as_millis());
        info!("[{} ms] [from_account_data] Декодирование аккаунта завершено ({} ms).", start.elapsed().as_millis(), start.elapsed().as_millis());
        Ok(BondingCurveState {
            virtual_token_reserves,
            virtual_sol_reserves,
            real_token_reserves,
            real_sol_reserves,
            token_total_supply,
            complete,
        })
    }
}

async fn get_pump_curve_state(client: &RpcClient, curve_address: &Pubkey) -> Result<BondingCurveState> {
    let start = Instant::now();
    info!("[{} ms] [get_pump_curve_state] Запрос данных для адреса {}.", start.elapsed().as_millis(), curve_address);
    let account_data = client.get_account_data(curve_address).await?;
    info!("[{} ms] [get_pump_curve_state] Данные аккаунта получены ({} ms).", start.elapsed().as_millis(), start.elapsed().as_millis());
    let state = BondingCurveState::from_account_data(&account_data)?;
    info!("[{} ms] [get_pump_curve_state] Состояние bonding curve разобрано.", start.elapsed().as_millis());
    Ok(state)
}

fn calculate_pump_curve_price(state: &BondingCurveState) -> Result<f64> {
    let start = Instant::now();
    info!("[{} ms] [calculate_pump_curve_price] Начало вычисления цены.", start.elapsed().as_millis());
    if state.virtual_token_reserves == 0 || state.virtual_sol_reserves == 0 {
        return Err(anyhow!("Некорректное состояние резервов"));
    }
    let sol = state.virtual_sol_reserves as f64 / LAMPORTS_PER_SOL as f64;
    let tokens = state.virtual_token_reserves as f64 / 10f64.powi(TOKEN_DECIMALS as i32);
    let price = sol / tokens;
    info!("[{} ms] [calculate_pump_curve_price] Цена токена вычислена: {:.8} SOL ({} ms).", start.elapsed().as_millis(), price, start.elapsed().as_millis());
    Ok(price)
}

// ====================================================================
// Функция покупки токена с подробным логированием каждого шага
async fn buy_token(
    client1: &RpcClient,
    client: &RpcClient,
    mint: Pubkey,
    bonding_curve: Pubkey,
    associated_bonding_curve: Pubkey,
    amount: f64,
    slippage: f64,
    max_retries: usize,
    global_start: &Instant,
    latest_blockhash: Arc<Mutex<Hash>>,
) -> Result<()> {
    let overall_start = Instant::now();
    info!("[{} ms] [buy_token] >>> Запуск покупки токена.", global_start.elapsed().as_millis());

    // 1. Декодирование приватного ключа
    let pk_start = Instant::now();
    let private_key_base58 = "4NNLZoqdEohpgeFnbfuhKNmQeEJUmEgbrViShMwhdBD2EyA1nspYR8jqaqyaYeUTbqtEVtfqR1dk4MFzdtrC5TJH";
    let private_key_bytes = private_key_base58
        .from_base58()
        .map_err(|e| anyhow!("Ошибка декодирования приватного ключа: {:?}", e))?;
    info!("[{} ms] [buy_token] Приватный ключ декодирован ({} ms).", global_start.elapsed().as_millis(), pk_start.elapsed().as_millis());

    // 2. Создание keypair
    let kp_start = Instant::now();
    let payer = Keypair::from_bytes(&private_key_bytes)
        .map_err(|e| anyhow!("Ошибка создания keypair: {:?}", e))?;
    info!("[{} ms] [buy_token] Keypair создан ({} ms).", global_start.elapsed().as_millis(), kp_start.elapsed().as_millis());

    // 3. Инструкция compute unit
    let compute_start = Instant::now();
    let optimal_compute_units: u32 = 90_000;
    let compute_limit_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(optimal_compute_units);
    let compute_unit_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(33_333_335);
    info!("[{} ms] [buy_token] Инструкция compute unit сформирована ({} ms).", global_start.elapsed().as_millis(), compute_start.elapsed().as_millis());

    // 4. Получение ATA для токена
    let ata_start = Instant::now();
    let associated_token_account = get_associated_token_address(&payer.pubkey(), &mint);
    info!("[{} ms] [buy_token] ATA адрес получен: {} ({} ms).", global_start.elapsed().as_millis(), associated_token_account, ata_start.elapsed().as_millis());

    // 5. Пересчёт SOL в лампорты
    let lamports_start = Instant::now();
    let amount_lamports = (amount * LAMPORTS_PER_SOL as f64) as u64;
    info!("[{} ms] [buy_token] SOL -> лампорты: {} ({} ms).", global_start.elapsed().as_millis(), amount_lamports, lamports_start.elapsed().as_millis());

    // 7. Вычисление цены токена (здесь для примера используется фиксированное значение)
    let price_start = Instant::now();
    let token_price_sol = 0.000000275;
    info!("[{} ms] [buy_token] Цена токена: {:.8} SOL ({} ms).", global_start.elapsed().as_millis(), token_price_sol, price_start.elapsed().as_millis());

    // 8. Вычисление количества токенов для покупки
    let token_amount_start = Instant::now();
    let token_amount = amount / token_price_sol;
    let token_amount_ui = (token_amount * 10f64.powi(TOKEN_DECIMALS as i32)).round() as u64;
    info!("[{} ms] [buy_token] Количество токенов: {} ({} ms).", global_start.elapsed().as_millis(), token_amount_ui, token_amount_start.elapsed().as_millis());

    // 9. Вычисление максимально допустимого списания с учётом slippage
    let max_lamports_start = Instant::now();
    let max_amount_lamports = (amount_lamports as f64 * (1.0 + slippage)).round() as u64;
    info!("[{} ms] [buy_token] Максимальное списание: {} лампортов ({} ms).", global_start.elapsed().as_millis(), max_amount_lamports, max_lamports_start.elapsed().as_millis());

    // 10. Инструкция для создания ATA (если необходимо)
    let create_ata_start = Instant::now();
    let create_ata_ix = spl_associated_token_account::instruction::create_associated_token_account(
        &payer.pubkey(),
        &payer.pubkey(),
        &mint,
        &spl_token::id(),
    );
    info!("[{} ms] [buy_token] Инструкция создания ATA сформирована ({} ms).", global_start.elapsed().as_millis(), create_ata_start.elapsed().as_millis());

    // 11. Формирование списка счетов для покупки
    let accounts_start = Instant::now();
    let accounts = vec![
        AccountMeta::new_readonly(*PUMP_GLOBAL, false),
        AccountMeta::new(*PUMP_FEE, false),
        AccountMeta::new_readonly(mint, false),
        AccountMeta::new(bonding_curve, false),
        AccountMeta::new(associated_bonding_curve, false),
        AccountMeta::new(associated_token_account, false),
        AccountMeta::new(payer.pubkey(), true),
        AccountMeta::new_readonly(system_program::id(), false),
        AccountMeta::new_readonly(spl_token::id(), false),
        AccountMeta::new_readonly(solana_sdk::sysvar::rent::id(), false),
        AccountMeta::new_readonly(*PUMP_EVENT_AUTHORITY, false),
        AccountMeta::new_readonly(*PUMP_PROGRAM, false),
    ];
    info!("[{} ms] [buy_token] Список счетов сформирован ({} ms).", global_start.elapsed().as_millis(), accounts_start.elapsed().as_millis());

    // 12. Формирование данных для инструкции покупки
    let data_start = Instant::now();
    let discriminator: [u8; 8] = 16927863322537952870u64.to_le_bytes();
    let mut data = Vec::new();
    data.extend_from_slice(&discriminator);
    data.extend_from_slice(&token_amount_ui.to_le_bytes());
    data.extend_from_slice(&max_amount_lamports.to_le_bytes());
    info!("[{} ms] [buy_token] Данные для инструкции покупки сформированы ({} ms).", global_start.elapsed().as_millis(), data_start.elapsed().as_millis());

    // 13. Формирование инструкции покупки
    let instr_start = Instant::now();
    let buy_ix = Instruction {
        program_id: *PUMP_PROGRAM,
        accounts,
        data,
    };
    info!("[{} ms] [buy_token] Инструкция покупки сформирована ({} ms).", global_start.elapsed().as_millis(), instr_start.elapsed().as_millis());

    // 14. Формирование транзакции
    let txn_start = Instant::now();
    let mut transaction = Transaction::new_with_payer(
        &[compute_limit_ix, compute_unit_ix, create_ata_ix, buy_ix],
        Some(&payer.pubkey()),
    );
    info!("[{} ms] [buy_token] Транзакция сформирована ({} ms).", global_start.elapsed().as_millis(), txn_start.elapsed().as_millis());

    // 15. Получение blockhash из разделяемой переменной (обновляется в фоне)
    let bh_start = Instant::now();
    let recent_blockhash = {
        let bh_lock = latest_blockhash.lock().await;
        *bh_lock
    };
    info!("[{} ms] [buy_token] Используем последний blockhash: {} ({} ms).", global_start.elapsed().as_millis(), recent_blockhash, bh_start.elapsed().as_millis());

    // 16. Подписание транзакции
    let sign_start = Instant::now();
    transaction.sign(&[&payer], recent_blockhash);
    info!("[{} ms] [buy_token] Транзакция подписана ({} ms).", global_start.elapsed().as_millis(), sign_start.elapsed().as_millis());

    // 17. Отправка транзакции с повторными попытками
    for attempt in 0..max_retries {
        let send_start = Instant::now();
        match client.send_transaction(&transaction).await {
            Ok(signature) => {
                info!("[{} ms] [buy_token] Транзакция отправлена: {} (попытка {} за {} ms).", global_start.elapsed().as_millis(), signature, attempt + 1, send_start.elapsed().as_millis());
                info!("[{} ms] [buy_token] >>> Общая длительность покупки: {} ms.", global_start.elapsed().as_millis(), overall_start.elapsed().as_millis());
                return Ok(());
            },
            Err(e) => {
                error!("[{} ms] [buy_token] Попытка {} не удалась: {:?} ({} ms).", global_start.elapsed().as_millis(), attempt + 1, e, send_start.elapsed().as_millis());
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
    Err(anyhow!("Не удалось отправить транзакцию после {} попыток", max_retries))
}

fn setup_logger() -> Result<(), Box<dyn std::error::Error>> {
    // Настройка формата логов, уровня логирования и цепочек вывода
    let log_file = OpenOptions::new()
    .write(true)
    .create(true)
    .truncate(true) // Очистка файла, если он уже существует
    .open("output.log")?;

Dispatch::new()
    .format(|out, message, record| {
        out.finish(format_args!(
            "{} [{}] {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            message
        ))
    })
    .level(log::LevelFilter::Info)
    .chain(log_file)
    .chain(std::io::stdout())
    .apply()?;
    Ok(())
}

// ====================================================================
// Основная функция: детальное логирование работы WebSocket и вызова покупки

#[tokio::main]
async fn main() -> Result<()> {
    let global_start = Instant::now();
    // Настройка логгера – теперь все сообщения будут писаться в консоль и файл
    setup_logger().expect("Ошибка настройки логгера");
    info!("[{} ms] [main] >>> Запуск программы.", global_start.elapsed().as_millis());

    // Инициализация RPC-клиента
    let rpc_init_start = Instant::now();
    let rpc_client = RpcClient::new_with_commitment(
        RPC_ENDPOINT.to_string(),
        CommitmentConfig::confirmed(),
    );
    let rpc_client1 = RpcClient::new_with_commitment(
        RPC_ENDPOINT1.to_string(),
        CommitmentConfig::confirmed(),
    );
    info!("[{} ms] [main] RPC-клиент инициализирован ({} ms).", global_start.elapsed().as_millis(), rpc_init_start.elapsed().as_millis());
    
    // Создаём разделяемую переменную для хранения последнего blockhash.
    let initial_blockhash = fetch_latest_blockhash(RPC_ENDPOINT1).await?;
    let latest_blockhash = Arc::new(Mutex::new(initial_blockhash));
    // Спавним задачу, которая будет обновлять blockhash каждые 400 мс.
    tokio::spawn(poll_latest_blockhash(RPC_ENDPOINT1, latest_blockhash.clone()));

    // Глобальное хранилище известных mint'ов
    let known_mints = Arc::new(Mutex::new(HashSet::<String>::new()));

    // Получение начального списка mint'ов
    let init_mints_start = Instant::now();
    let initial_mints = get_token_mints(HELIUS_HTTP_URL, OWNER_PUBKEY).await?;
    {
        let mut set = known_mints.lock().await;
        for mint in initial_mints {
            set.insert(mint);
        }
    }
    info!("[{} ms] [main] Начальный список mint'ов получен ({} ms).", global_start.elapsed().as_millis(), init_mints_start.elapsed().as_millis());

    // Основной цикл подключения к WebSocket
    loop {
        info!("[{} ms] [main] Подключаемся к WS: {}", global_start.elapsed().as_millis(), HELIUS_WS_URL);
        let ws_conn_start = Instant::now();
        match connect_async(HELIUS_WS_URL).await {
            Ok((ws_stream, _)) => {
                info!("[{} ms] [main] WS-соединение установлено ({} ms).", global_start.elapsed().as_millis(), ws_conn_start.elapsed().as_millis());
                let (mut write, mut read) = ws_stream.split();

                // Отправка запроса на подписку
                let sub_req_start = Instant::now();
                let sub_request = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        { "mentions": [OWNER_PUBKEY] },
                        { "commitment": "processed" }
                    ]
                });
                if let Err(e) = write.send(Message::Text(sub_request.to_string())).await {
                    error!("[{} ms] [main] Ошибка отправки запроса подписки: {}.", global_start.elapsed().as_millis(), e);
                    continue;
                }
                info!("[{} ms] [main] Запрос на подписку отправлен ({} ms).", global_start.elapsed().as_millis(), sub_req_start.elapsed().as_millis());

                // Интервал для отправки ping каждые 30 секунд
                let mut ping_interval = interval(Duration::from_secs(30));

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            let ping_start = Instant::now();
                            if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                                error!("[{} ms] [main] Ошибка отправки ping: {}.", global_start.elapsed().as_millis(), e);
                                break;
                            } else {
                                info!("[{} ms] [main] Ping отправлен ({} ms).", global_start.elapsed().as_millis(), ping_start.elapsed().as_millis());
                            }
                        }
                        msg = read.next() => {
                            let msg_rcv_start = Instant::now();
                            match msg {
                                Some(Ok(message)) => {
                                    match message {
                                        Message::Text(text) => {
                                            info!("[{} ms] [main] Получено WS-сообщение: {}.", global_start.elapsed().as_millis(), text);
                                            // При получении сообщения запрашиваем обновлённый список mint'ов
                                            let api_start = Instant::now();
                                            match get_token_mints(HELIUS_HTTP_URL, OWNER_PUBKEY).await {
                                                Ok(new_mints) => {
                                                    info!("[{} ms] [main] API вызов getTokenAccountsByOwner выполнен ({} ms).", global_start.elapsed().as_millis(), api_start.elapsed().as_millis());
                                                    let mut set = known_mints.lock().await;
                                                    for mint in new_mints {
                                                        if !set.contains(&mint) {
                                                            info!("[{} ms] [main] Новый mint обнаружен: {}.", global_start.elapsed().as_millis(), mint);
                                                            set.insert(mint.clone());
                                                            match Pubkey::from_str(&mint) {
                                                                Ok(mint_pubkey) => {
                                                                    let (bonding_curve, _bump) = get_bonding_curve_address(&mint_pubkey, &PUMP_PROGRAM);
                                                                    let associated_bonding_curve = find_associated_bonding_curve(&mint_pubkey, &bonding_curve);
                                                                    let buy_start = Instant::now();
                                                                    match buy_token(
                                                                        &rpc_client1,
                                                                        &rpc_client,
                                                                        mint_pubkey,
                                                                        bonding_curve,
                                                                        associated_bonding_curve,
                                                                        BUY_AMOUNT,
                                                                        BUY_SLIPPAGE,
                                                                        MAX_RETRIES,
                                                                        &global_start,
                                                                        latest_blockhash.clone(), // передаём разделяемую переменную
                                                                    ).await {
                                                                        Ok(_) => info!("[{} ms] [main] Покупка токена для mint {} выполнена успешно ({} ms).", global_start.elapsed().as_millis(), mint, buy_start.elapsed().as_millis()),
                                                                        Err(e) => error!("[{} ms] [main] Ошибка при покупке токена для mint {}: {:?} ({} ms).", global_start.elapsed().as_millis(), mint, e, buy_start.elapsed().as_millis()),
                                                                    }
                                                                },
                                                                Err(e) => {
                                                                    error!("[{} ms] [main] Ошибка преобразования mint в Pubkey: {:?}.", global_start.elapsed().as_millis(), e);
                                                                }
                                                            }
                                                        } 
                                                        else {
                                                            info!("[{} ms] [main] Mint {} уже обработан.", global_start.elapsed().as_millis(), mint);
                                                        }
                                                    }
                                                },
                                                Err(e) => {
                                                    error!("[{} ms] [main] Ошибка API getTokenAccountsByOwner: {}.", global_start.elapsed().as_millis(), e);
                                                }
                                            }
                                        }
                                        Message::Ping(payload) => {
                                            info!("[{} ms] [main] Получен Ping: {:?}.", global_start.elapsed().as_millis(), payload);
                                            if let Err(e) = write.send(Message::Pong(payload)).await {
                                                error!("[{} ms] [main] Ошибка отправки Pong: {}.", global_start.elapsed().as_millis(), e);
                                            }
                                        }
                                        Message::Pong(_) => {
                                            info!("[{} ms] [main] Получен Pong.", global_start.elapsed().as_millis());
                                        }
                                        Message::Close(reason) => {
                                            info!("[{} ms] [main] WS-соединение закрыто: {:?}.", global_start.elapsed().as_millis(), reason);
                                            break;
                                        }
                                        Message::Binary(bin) => {
                                            info!("[{} ms] [main] Получено бинарное сообщение: {:?}.", global_start.elapsed().as_millis(), bin);
                                        }
                                        _ => {
                                            info!("[{} ms] [main] Получено неизвестное сообщение.", global_start.elapsed().as_millis());
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("[{} ms] [main] Ошибка чтения WS-сообщения: {}.", global_start.elapsed().as_millis(), e);
                                    break;
                                }
                                None => {
                                    info!("[{} ms] [main] WS-соединение завершилось.", global_start.elapsed().as_millis());
                                    break;
                                }
                            }
                            info!("[{} ms] [main] Обработка WS-сообщения завершена ({} ms).", global_start.elapsed().as_millis(), msg_rcv_start.elapsed().as_millis());
                        }
                    }
                }
                info!("[{} ms] [main] WS-соединение потеряно. Переподключаемся через 5 секунд...", global_start.elapsed().as_millis());
                sleep(Duration::from_secs(5)).await;
            }
            Err(e) => {
                error!("[{} ms] [main] Ошибка подключения к WS: {}.", global_start.elapsed().as_millis(), e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
//ya izmenil