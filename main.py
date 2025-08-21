from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import aiohttp
import asyncio
import logging
from datetime import datetime
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Extraction Service", version="1.0.0")

# 数据模型
class SiteConfig(BaseModel):
    name: str
    client_id: str
    key: str
    propertyId: str
    categoryId: str

class ExtractRequest(BaseModel):
    hasura_url: str
    start: str
    end: str
    propertyvalue: str
    all_sites: List[SiteConfig]

class DataExtractor:
    def __init__(self):
        self.session = None
        self.token_url = "https://api.kolable.app/api/v1/auth/token"  # 固定token端点
        self.query_url = "https://rhdb.kolable.com/v1/graphql"       # 固定查询端点
        self.max_retries = 10
    
    async def create_session(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None
    
    async def get_site_token(self, site_config: SiteConfig) -> Optional[str]:
        """獲取站點token - 使用固定端點"""
        session = await self.create_session()
        
        # 按照你指定的格式構建請求
        token_payload = {
            "clientId": site_config.client_id,
            "key": site_config.key,
            "permission": []
        }
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"[{site_config.name}] 獲取token - 嘗試 {attempt}/{self.max_retries}")
                logger.info(f"[{site_config.name}] 請求數據: {token_payload}")
                
                async with session.post(
                    self.token_url,
                    json=token_payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        # 修正：token在result.authToken裡面
                        token = None
                        if result.get("result") and result["result"].get("authToken"):
                            token = result["result"]["authToken"]
                        elif result.get("token"):  # 備用方案，以防其他站點格式不同
                            token = result["token"]
                        
                        if token:
                            logger.info(f"[{site_config.name}] ✅ 成功獲取token")
                            return token
                        else:
                            logger.warning(f"[{site_config.name}] ⚠️ 響應中沒有找到token: {result}")
                    else:
                        error_text = await response.text()
                        logger.warning(f"[{site_config.name}] ❌ 獲取token失敗 {response.status}: {error_text}")
                        
            except Exception as e:
                logger.warning(f"[{site_config.name}] 獲取token異常: {str(e)}")
            
            if attempt < self.max_retries:
                wait_time = min(2 ** attempt, 30)
                logger.info(f"[{site_config.name}] 等待 {wait_time} 秒後重試...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"[{site_config.name}] 獲取token失敗，已達到最大重試次數")
        return None
    
    async def query_site_data(self, site_config: SiteConfig, token: str, propertyvalue: str, start: str, end: str):
        """查詢站點數據 - 使用固定端點和站點自己的propertyId，包含會員編號"""
        session = await self.create_session()
        
        # 構建GraphQL查詢 - 修改為包含會員ID和其他會員資訊
        graphql_query = {
            "operationName": "SearchMemberNotesWithCountAndTime",
            "query": """query SearchMemberNotesWithCountAndTime($propertyId: uuid!, $value: String!, $start: timestamptz!, $end: timestamptz!) {
  member_aggregate(
    where: {
      member_properties: { property_id: { _eq: $propertyId }, value: { _eq: $value } },
      created_at: { _gte: $start, _lte: $end },
      member_notes: { description: { _is_null: false, _neq: "" } }
    }
  ) {
    aggregate {
      count
    }
  }
  member(
    where: {
      member_properties: { property_id: { _eq: $propertyId }, value: { _eq: $value } },
      created_at: { _gte: $start, _lte: $end },
      member_notes: { description: { _is_null: false, _neq: "" } }
    }
  ) {
    id
    username
    name
    email
    created_at
    member_notes(where: { description: { _is_null: false, _neq: "" } }, order_by: { created_at: desc }) {
      id
      created_at
      description
    }
  }
}""",
            "variables": {
                "propertyId": site_config.propertyId,  # 使用站點自己的propertyId
                "value": propertyvalue,                # 使用POST傳入的propertyvalue
                "start": start,                        # 使用POST傳入的start
                "end": end                            # 使用POST傳入的end
            }
        }
        
        # 設置請求頭 - 使用Bearer token
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"[{site_config.name}] 查詢數據 - 嘗試 {attempt}/{self.max_retries}")
                logger.info(f"[{site_config.name}] 查詢參數: propertyId={site_config.propertyId}, value={propertyvalue}")
                
                async with session.post(
                    self.query_url,  # 使用固定的查詢端點
                    json=graphql_query,
                    headers=headers
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        
                        if result.get("errors"):
                            logger.warning(f"[{site_config.name}] GraphQL錯誤: {result['errors']}")
                            return {"error": "GraphQL錯誤", "site": site_config.name, "details": result['errors']}
                        
                        if "data" in result:
                            logger.info(f"[{site_config.name}] ✅ 成功查詢數據")
                            result["site_info"] = {
                                "name": site_config.name,
                                "client_id": site_config.client_id,
                                "propertyId": site_config.propertyId,
                                "categoryId": site_config.categoryId
                            }
                            return result
                    else:
                        error_text = await response.text()
                        logger.warning(f"[{site_config.name}] 查詢失敗 {response.status}: {error_text}")
                        
            except Exception as e:
                logger.warning(f"[{site_config.name}] 查詢異常: {str(e)}")
            
            if attempt < self.max_retries:
                wait_time = min(2 ** attempt, 30)
                logger.info(f"[{site_config.name}] 等待 {wait_time} 秒後重試...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"[{site_config.name}] 查詢數據失敗，已達到最大重試次數")
        return {"error": "查詢失敗", "site": site_config.name}
    
    async def process_single_site(self, site_config: SiteConfig, propertyvalue: str, start: str, end: str):
        """處理單個站點：獲取token -> 查詢數據"""
        logger.info(f"[{site_config.name}] 🚀 開始處理站點")
        
        # 步驟1: 獲取token
        token = await self.get_site_token(site_config)
        if not token:
            return {"error": "無法獲取token", "site": site_config.name}
        
        # 步驟 2: 查詢數據
        result = await self.query_site_data(site_config, token, propertyvalue, start, end)
        return result
    
    async def extract_all_sites_data(self, request_data: ExtractRequest):
        """並發處理所有站點"""
        logger.info(f"🎯 開始處理 {len(request_data.all_sites)} 個站點")
        logger.info(f"📅 時間範圍: {request_data.start} 到 {request_data.end}")
        logger.info(f"🔍 查詢屬性值: {request_data.propertyvalue}")
        
        try:
            # 並發處理所有站點
            tasks = []
            for site in request_data.all_sites:
                task = self.process_single_site(
                    site, 
                    request_data.propertyvalue, 
                    request_data.start, 
                    request_data.end
                )
                tasks.append(task)
            
            logger.info("⏳ 等待所有站點處理完成...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 合併結果
            merged_data = {
                "success_count": 0,
                "error_count": 0,
                "total_member_count": 0,
                "total_notes_count": 0,
                "sites_data": [],
                "errors": [],
                "merged_notes": [],
                "members_info": []  # 新增：會員資訊列表
            }
            
            for result in results:
                if isinstance(result, Exception):
                    merged_data["error_count"] += 1
                    merged_data["errors"].append({"error": "Exception", "message": str(result)})
                    logger.error(f"💥 處理異常: {str(result)}")
                    continue
                
                if "error" in result:
                    merged_data["error_count"] += 1
                    merged_data["errors"].append(result)
                    logger.error(f"❌ 站點 {result.get('site', 'unknown')} 處理失敗")
                    continue
                
                if "data" in result:
                    merged_data["success_count"] += 1
                    site_info = result.get("site_info", {})
                    
                    # 統計會員數
                    member_count = 0
                    if result["data"].get("member_aggregate"):
                        member_count = result["data"]["member_aggregate"]["aggregate"]["count"]
                        merged_data["total_member_count"] += member_count
                    
                    # 收集笔记和會員資訊
                    site_notes = []
                    site_members = []
                    
                    if result["data"].get("member"):
                        for member in result["data"]["member"]:
                            # 收集會員基本資訊
                            member_info = {
                                "member_id": member.get("id"),
                                "username": member.get("username"),
                                "name": member.get("name"),
                                "email": member.get("email"),
                                "member_created_at": member.get("created_at"),
                                "site": site_info.get("name", "unknown"),
                                "site_client_id": site_info.get("client_id"),
                                "notes_count": len(member.get("member_notes", []))
                            }
                            site_members.append(member_info)
                            merged_data["members_info"].append(member_info)
                            
                            # 收集筆記（包含會員資訊）
                            if member.get("member_notes"):
                                for note in member["member_notes"]:
                                    note_with_member = {
                                        "note_id": note.get("id"),
                                        "created_at": note.get("created_at"),
                                        "description": note.get("description"),
                                        "member_id": member.get("id"),
                                        "member_username": member.get("username"),
                                        "member_name": member.get("name"),
                                        "member_email": member.get("email"),
                                        "site": site_info.get("name", "unknown")
                                    }
                                    site_notes.append(note_with_member)
                                    merged_data["merged_notes"].append(note_with_member)
                    
                    merged_data["sites_data"].append({
                        "site_info": site_info,
                        "member_count": member_count,
                        "notes_count": len(site_notes),
                        "notes": site_notes,
                        "members": site_members  # 新增：該站點的會員列表
                    })
                    
                    logger.info(f"✅ 站點 {site_info.get('name', 'unknown')} 處理完成: {member_count} 會員, {len(site_notes)} 筆記")
            
            merged_data["total_notes_count"] = len(merged_data["merged_notes"])
            
            # 按時間排序合併的筆記
            merged_data["merged_notes"].sort(key=lambda x: x.get("created_at", ""), reverse=True)
            
            # 按會員ID排序會員資訊
            merged_data["members_info"].sort(key=lambda x: (x.get("site", ""), x.get("member_id", "")))
            
            logger.info(f"🏁 處理完成: 成功 {merged_data['success_count']}, 失敗 {merged_data['error_count']}")
            logger.info(f"📊 總計: {merged_data['total_member_count']} 會員, {merged_data['total_notes_count']} 筆記")
            
            return merged_data
            
        finally:
            await self.close_session()

extractor = DataExtractor()

@app.get("/")
async def root():
    return {
        "service": "Data Extraction Service", 
        "status": "running",
        "endpoints": {
            "token_url": extractor.token_url,
            "query_url": extractor.query_url
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/extract-data")
async def extract_data(request_data: ExtractRequest):
    try:
        logger.info(f"🎯 收到數據提取請求")
        logger.info(f"📋 請求參數: 站點數={len(request_data.all_sites)}, 屬性值={request_data.propertyvalue}")
        
        result = await extractor.extract_all_sites_data(request_data)
        
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "request_info": {
                "propertyvalue": request_data.propertyvalue,
                "start": request_data.start,
                "end": request_data.end,
                "sites_count": len(request_data.all_sites)
            },
            "data": result
        }
        
    except Exception as e:
        logger.error(f"💥 處理請求出錯: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Cloud Run 使用 PORT 環境變量，默認是 8080
    port = int(os.environ.get('PORT', 8080))  # 改為 8080
    uvicorn.run(app, host="0.0.0.0", port=port)
