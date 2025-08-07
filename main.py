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
        """获取站点token - 使用固定端点"""
        session = await self.create_session()
        
        # 按照你指定的格式构建请求
        token_payload = {
            "clientId": site_config.client_id,
            "key": site_config.key,
            "permission": []
        }
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"[{site_config.name}] 获取token - 尝试 {attempt}/{self.max_retries}")
                logger.info(f"[{site_config.name}] 请求数据: {token_payload}")
                
                async with session.post(
                    self.token_url,
                    json=token_payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        # 修正：token在result.authToken里面
                        token = None
                        if result.get("result") and result["result"].get("authToken"):
                            token = result["result"]["authToken"]
                        elif result.get("token"):  # 备用方案，以防其他站点格式不同
                            token = result["token"]
                        
                        if token:
                            logger.info(f"[{site_config.name}] ✅ 成功获取token")
                            return token
                        else:
                            logger.warning(f"[{site_config.name}] ⚠️ 响应中没有找到token: {result}")
                    else:
                        error_text = await response.text()
                        logger.warning(f"[{site_config.name}] ❌ 获取token失败 {response.status}: {error_text}")
                        
            except Exception as e:
                logger.warning(f"[{site_config.name}] 获取token异常: {str(e)}")
            
            if attempt < self.max_retries:
                wait_time = min(2 ** attempt, 30)
                logger.info(f"[{site_config.name}] 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"[{site_config.name}] 获取token失败，已达到最大重试次数")
        return None
    
    async def query_site_data(self, site_config: SiteConfig, token: str, propertyvalue: str, start: str, end: str):
        """查询站点数据 - 使用固定端点和站点自己的propertyId"""
        session = await self.create_session()
        
        # 构建GraphQL查询 - 使用站点自己的propertyId
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
    member_notes(where: { description: { _is_null: false, _neq: "" } }, order_by: { created_at: desc }) {
      created_at
      description
    }
  }
}""",
            "variables": {
                "propertyId": site_config.propertyId,  # 使用站点自己的propertyId
                "value": propertyvalue,                # 使用POST传入的propertyvalue
                "start": start,                        # 使用POST传入的start
                "end": end                            # 使用POST传入的end
            }
        }
        
        # 设置请求头 - 使用Bearer token
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"[{site_config.name}] 查询数据 - 尝试 {attempt}/{self.max_retries}")
                logger.info(f"[{site_config.name}] 查询参数: propertyId={site_config.propertyId}, value={propertyvalue}")
                
                async with session.post(
                    self.query_url,  # 使用固定的查询端点
                    json=graphql_query,
                    headers=headers
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        
                        if result.get("errors"):
                            logger.warning(f"[{site_config.name}] GraphQL错误: {result['errors']}")
                            return {"error": "GraphQL错误", "site": site_config.name, "details": result['errors']}
                        
                        if "data" in result:
                            logger.info(f"[{site_config.name}] ✅ 成功查询数据")
                            result["site_info"] = {
                                "name": site_config.name,
                                "client_id": site_config.client_id,
                                "propertyId": site_config.propertyId,
                                "categoryId": site_config.categoryId
                            }
                            return result
                    else:
                        error_text = await response.text()
                        logger.warning(f"[{site_config.name}] 查询失败 {response.status}: {error_text}")
                        
            except Exception as e:
                logger.warning(f"[{site_config.name}] 查询异常: {str(e)}")
            
            if attempt < self.max_retries:
                wait_time = min(2 ** attempt, 30)
                logger.info(f"[{site_config.name}] 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"[{site_config.name}] 查询数据失败，已达到最大重试次数")
        return {"error": "查询失败", "site": site_config.name}
    
    async def process_single_site(self, site_config: SiteConfig, propertyvalue: str, start: str, end: str):
        """处理单个站点：获取token -> 查询数据"""
        logger.info(f"[{site_config.name}] 🚀 开始处理站点")
        
        # 步骤1: 获取token
        token = await self.get_site_token(site_config)
        if not token:
            return {"error": "无法获取token", "site": site_config.name}
        
        # 步骤 2: 查询数据
        result = await self.query_site_data(site_config, token, propertyvalue, start, end)
        return result
    
    async def extract_all_sites_data(self, request_data: ExtractRequest):
        """并发处理所有站点"""
        logger.info(f"🎯 开始处理 {len(request_data.all_sites)} 个站点")
        logger.info(f"📅 时间范围: {request_data.start} 到 {request_data.end}")
        logger.info(f"🔍 查询属性值: {request_data.propertyvalue}")
        
        try:
            # 并发处理所有站点
            tasks = []
            for site in request_data.all_sites:
                task = self.process_single_site(
                    site, 
                    request_data.propertyvalue, 
                    request_data.start, 
                    request_data.end
                )
                tasks.append(task)
            
            logger.info("⏳ 等待所有站点处理完成...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 合并结果
            merged_data = {
                "success_count": 0,
                "error_count": 0,
                "total_member_count": 0,
                "total_notes_count": 0,
                "sites_data": [],
                "errors": [],
                "merged_notes": []
            }
            
            for result in results:
                if isinstance(result, Exception):
                    merged_data["error_count"] += 1
                    merged_data["errors"].append({"error": "Exception", "message": str(result)})
                    logger.error(f"💥 处理异常: {str(result)}")
                    continue
                
                if "error" in result:
                    merged_data["error_count"] += 1
                    merged_data["errors"].append(result)
                    logger.error(f"❌ 站点 {result.get('site', 'unknown')} 处理失败")
                    continue
                
                if "data" in result:
                    merged_data["success_count"] += 1
                    site_info = result.get("site_info", {})
                    
                    # 统计会员数
                    member_count = 0
                    if result["data"].get("member_aggregate"):
                        member_count = result["data"]["member_aggregate"]["aggregate"]["count"]
                        merged_data["total_member_count"] += member_count
                    
                    # 收集笔记
                    site_notes = []
                    if result["data"].get("member"):
                        for member in result["data"]["member"]:
                            if member.get("member_notes"):
                                for note in member["member_notes"]:
                                    note_with_site = {
                                        **note,
                                        "site": site_info.get("name", "unknown")
                                    }
                                    site_notes.append(note_with_site)
                                    merged_data["merged_notes"].append(note_with_site)
                    
                    merged_data["sites_data"].append({
                        "site_info": site_info,
                        "member_count": member_count,
                        "notes_count": len(site_notes),
                        "notes": site_notes
                    })
                    
                    logger.info(f"✅ 站点 {site_info.get('name', 'unknown')} 处理完成: {member_count} 会员, {len(site_notes)} 笔记")
            
            merged_data["total_notes_count"] = len(merged_data["merged_notes"])
            
            # 按时间排序合并的笔记
            merged_data["merged_notes"].sort(key=lambda x: x.get("created_at", ""), reverse=True)
            
            logger.info(f"🏁 处理完成: 成功 {merged_data['success_count']}, 失败 {merged_data['error_count']}")
            logger.info(f"📊 总计: {merged_data['total_member_count']} 会员, {merged_data['total_notes_count']} 笔记")
            
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
        logger.info(f"🎯 收到数据提取请求")
        logger.info(f"📋 请求参数: 站点数={len(request_data.all_sites)}, 属性值={request_data.propertyvalue}")
        
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
        logger.error(f"💥 处理请求出错: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Cloud Run 使用 PORT 环境变量，默认是 8080
    port = int(os.environ.get('PORT', 8080))  # 改为 8080
    uvicorn.run(app, host="0.0.0.0", port=port)
