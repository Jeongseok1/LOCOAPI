import Redis from "redis";
import ComprehensiveEncryption from "../encryption/comprehensiveEncryption.js";

class IntelligentCache {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.memoryCache = new Map();
    this.memoryCleanupInterval = null;
    this.initializeRedis(); // 비동기 초기화 시작
    this.startMemoryCleanup();
  }

  // 복호화된 사용자 데이터 캐시 조회
  async getDecryptedUser(userId) {
    try {
      if (this.client) {
        const cached = await this.client.get(`decrypted_user:${userId}`);
        if (cached) {
          return JSON.parse(cached);
        }
      } else {
        return this.memoryCache.get(`decrypted_user:${userId}`);
      }
      return null;
    } catch (error) {
      console.error('복호화 사용자 캐시 조회 실패:', error);
      return null;
    }
  }

  // 복호화된 사용자 데이터 캐시 저장
  async cacheDecryptedUser(userId, decryptedUser) {
    try {
      const ttl = 3600; // 1시간
      
      if (this.client) {
        await this.client.setEx(
          `decrypted_user:${userId}`, 
          ttl, 
          JSON.stringify(decryptedUser)
        );
      } else {
        this.memoryCache.set(`decrypted_user:${userId}`, decryptedUser);
        
        // 메모리 캐시 만료 처리
        setTimeout(() => {
          this.memoryCache.delete(`decrypted_user:${userId}`);
        }, ttl * 1000);
      }
      
      console.log(`✅ 복호화 데이터 캐시 저장: ${userId}`);
    } catch (error) {
      console.error('복호화 사용자 캐시 저장 실패:', error);
    }
  }

  // 🔄 강제 Redis 재연결 (디버깅용)
  async forceRedisConnection() {
    console.log('🔄 강제 Redis 재연결 시도...');
    
    // 기존 연결 정리
    if (this.client) {
      try {
        await this.client.disconnect();
      } catch (e) {
        // 무시
      }
    }
    
    this.client = null;
    this.isConnected = false;
    
    // 새로 초기화
    await this.initializeRedis();
    
    return this.client ? 'Redis' : 'Memory';
  }

  // 연결 상태 확인
  getConnectionStatus() {
    return {
      type: this.client ? 'Redis' : 'Memory',
      isConnected: this.isConnected,
      clientExists: !!this.client
    };
  }

  async initializeRedis() {
    try {
      // 환경변수 확인을 더 엄격하게
      console.log('🔧 Redis 초기화 시작...');
      console.log('REDIS_HOST:', process.env.REDIS_HOST);
      console.log('ENABLE_CACHE:', process.env.ENABLE_CACHE);
      
      // Redis 설정이 없으면 메모리 캐시로 폴백
      if (!process.env.REDIS_HOST || process.env.ENABLE_CACHE !== 'true') {
        console.log('ℹ️ Redis 비활성화, 메모리 캐시 사용');
        this.isConnected = true;
        return;
      }

      console.log('📡 Redis 클라이언트 생성 중...');
      this.client = Redis.createClient({
        socket: {
          host: process.env.REDIS_HOST,
          port: parseInt(process.env.REDIS_PORT) || 6379,
          connectTimeout: 5000, // 5초 타임아웃
        },
        password: process.env.REDIS_PASSWORD,
      });

      this.client.on('error', (err) => {
        console.error('❌ Redis 클라이언트 오류:', err.message);
        this.fallbackToMemory();
      });

      this.client.on('connect', () => {
        console.log('🔗 Redis 연결 시도 중...');
      });

      this.client.on('ready', () => {
        console.log('✅ Redis 클라이언트 준비 완료');
      });

      console.log('🚀 Redis 연결 시작...');
      await this.client.connect();
      
      // 연결 테스트
      await this.client.ping();
      
      this.isConnected = true;
      console.log('🎉 Redis 연결 및 초기화 완료!');
      
    } catch (error) {
      console.error('❌ Redis 초기화 실패:', error.message);
      this.fallbackToMemory();
    }
  }

  fallbackToMemory() {
    console.log('🔄 메모리 캐시로 폴백');
    if (this.client) {
      this.client.disconnect().catch(() => {}); // 조용히 연결 끊기
    }
    this.client = null;
    this.isConnected = true; // 메모리 캐시로 사용 가능
  }

  // 통합 캐시 저장 메서드
  async setCache(key, data, ttl = 3600) {
    if (!this.isConnected) return false;

    try {
      const value = JSON.stringify(data);

      if (this.client) {
        // ✅ TTL이 0이거나 음수면 무제한 캐시 (SET 사용)
        if (ttl <= 0) {
          await this.client.set(key, value);
          console.log(`💾 [무제한 캐시] ${key}`);
        } else {
          // TTL이 양수면 만료 시간 설정 (SETEX 사용)
          await this.client.setEx(key, ttl, value);
        }
      } else if (this.memoryCache) {
        // 메모리 캐시는 TTL 0이면 무제한으로 저장
        if (ttl <= 0) {
          this.memoryCache.set(key, { value, expires: Infinity });
        } else {
          this.memoryCache.set(key, { value, expires: Date.now() + (ttl * 1000) });
        }
      }
      return true;
    } catch (error) {
      console.error('캐시 저장 실패:', error);
      return false;
    }
  }

  // 통합 캐시 조회 메서드
  async getCache(key) {
    if (!this.isConnected) return null;

    try {
      if (this.client) {
        const data = await this.client.get(key);
        return data ? JSON.parse(data) : null;
      } else if (this.memoryCache) {
        const cached = this.memoryCache.get(key);
        if (cached && cached.expires > Date.now()) {
          return JSON.parse(cached.value);
        } else if (cached) {
          this.memoryCache.delete(key); // 만료된 캐시 삭제
        }
      }
      return null;
    } catch (error) {
      console.error('캐시 조회 실패:', error);
      return null;
    }
  }

  // // 복호화된 사용자 정보 캐싱 (관리자용)
  // async cacheDecryptedUser(userId, decryptedUserData) {
  //   const key = `decrypted_user:${userId}`;
  //   await this.setCache(key, decryptedUserData, 1800); // 30분 TTL
  // }
  //
  // async getDecryptedUser(userId) {
  //   const key = `decrypted_user:${userId}`;
  //   return await this.getCache(key);
  // }

  // 🎯 계산된 나이 정보 캐싱 (24시간 TTL)
  async cacheUserAge(userId, age, ageGroup, isMinor) {
    const key = `user_age:${userId}`;
    const data = {
      age,
      ageGroup,
      isMinor,
      calculatedAt: new Date().toISOString(),
    };
    
    // 24시간 캐시 (나이는 자주 변하지 않음)
    await this.setCache(key, data, 86400);
  }

  async getCachedUserAge(userId) {
    const key = `user_age:${userId}`;
    const data = await this.getCache(key);
    
    if (!data) return null;

    // 선택적: 로깅용 (디버깅)
    const cacheAge = Date.now() - new Date(data.calculatedAt).getTime();
    const hoursOld = Math.floor(cacheAge / (1000 * 60 * 60));
    if (hoursOld > 0) {
      console.log(`💾 [나이 캐시] ${userId} - ${data.age}세 (캐싱된 지 ${hoursOld}시간)`);
    }

    return data;
  }
  // 채팅용 사용자 정보 캐싱 (나이 포함)
  async cacheChatUserInfo(userId, userInfo, birthdate = null) {
    const key = `chat_user:${userId}`;
    const data = {
      nickname: userInfo.nickname,
      profilePhoto: userInfo.profilePhoto,
      gender: userInfo.gender,
      star: userInfo.star,
    };

    // 🎯 생년월일이 있으면 나이 계산해서 캐시
    if (birthdate) {
      try {
        const decryptedBirthdate = ComprehensiveEncryption.decryptPersonalInfo(birthdate);
        if (decryptedBirthdate) {
          data.age = ComprehensiveEncryption.calculateAge(decryptedBirthdate);
          data.ageGroup = ComprehensiveEncryption.getAgeGroup(decryptedBirthdate);
          data.isMinor = ComprehensiveEncryption.isMinor(decryptedBirthdate);
        }
      } catch (error) {
        console.error('캐시 나이 계산 실패:', error);
      }
    }

    await this.setCache(key, data, 3600); // 1시간 TTL
  }

  async getChatUserInfo(userId) {
    const key = `chat_user:${userId}`;
    return await this.getCache(key);
  }

  // 배치 캐시 조회 (성능 최적화)
  async batchGetChatUserInfo(userIds) {
    const results = [];
    
    for (const userId of userIds) {
      const userInfo = await this.getChatUserInfo(userId);
      results.push({
        userId,
        userInfo
      });
    }
    
    return results;
  }

  // 🎯 나이 기반 매칭 캐시
  async cacheAgeGroupUsers(ageGroup, users) {
    const key = `age_group:${ageGroup}`;
    await this.setCache(key, users, 3600); // 1시간 TTL
  }

  async getCachedAgeGroupUsers(ageGroup) {
    const key = `age_group:${ageGroup}`;
    return await this.getCache(key);
  }

  // 온라인 사용자 상태 관리
  async setUserOnline(userId, socketId) {
    const key = `online:${userId}`;
    await this.setCache(key, socketId, 300); // 5분 TTL
  }

  async isUserOnline(userId) {
    const key = `online:${userId}`;
    const result = await this.getCache(key);
    return !!result;
  }

  async getOnlineUserCount() {
    if (!this.isConnected) return 0;

    try {
      if (this.client) {
        const keys = await this.client.keys('online:*');
        return keys.length;
      } else if (this.memoryCache) {
        let count = 0;
        for (const [key] of this.memoryCache) {
          if (key.startsWith('online:')) count++;
        }
        return count;
      }
      return 0;
    } catch (error) {
      console.error('온라인 사용자 수 조회 실패:', error);
      return 0;
    }
  }

  // 캐시 삭제
  async deleteCache(key) {
    if (!this.isConnected) return false;

    try {
      if (this.client) {
        await this.client.del(key);
        console.log(`🗑️ [캐시 삭제] ${key}`);
      } else if (this.memoryCache) {
        this.memoryCache.delete(key);
        console.log(`🗑️ [메모리 캐시 삭제] ${key}`);
      }
      return true;
    } catch (error) {
      console.error('캐시 삭제 실패:', error);
      return false;
    }
  }


  async scanKeys(pattern) {
    if (!this.isConnected) {
      console.warn('[Cache] SCAN 불가능 (연결 안 됨)');
      return [];
    }

    try {
      if (this.client) {
        // Redis에서 패턴 매칭 키 조회
        const keys = await this.client.keys(pattern);
        return keys;
      } else if (this.memoryCache) {
        // 메모리 캐시에서 패턴 매칭
        const matchedKeys = [];
        const regex = new RegExp('^' + pattern.replace('*', '.*') + '$');

        for (const [key] of this.memoryCache) {
          if (regex.test(key)) {
            matchedKeys.push(key);
          }
        }
        return matchedKeys;
      }
      return [];
    } catch (error) {
      console.error('[Cache] SCAN 실패:', error);
      return [];
    }
  }

  // 캐시 무효화 (사용자 정보 변경 시)
  async invalidateUserCache(userId) {
    const keys = [
      `decrypted_user:${userId}`, 
      `chat_user:${userId}`,
      `user_age:${userId}`
    ];
    
    for (const key of keys) {
      await this.deleteCache(key);
    }
  }

  // 캐시 통계
  async getCacheStats() {
    try {
      if (this.client) {
        const info = await this.client.info("stats");
        const keyspace = await this.client.info("keyspace");

        return {
          type: 'Redis',
          hitRate: this.extractHitRate(info),
          totalKeys: this.extractKeyCount(keyspace),
          memoryUsage: this.extractMemoryUsage(info),
          ageCache: {
            totalAgeEntries: await this.getKeyCount('user_age:*'),
            chatUserEntries: await this.getKeyCount('chat_user:*'),
          }
        };
      } else if (this.memoryCache) {
        return {
          type: 'Memory',
          totalKeys: this.memoryCache.size,
          ageCache: {
            totalAgeEntries: this.getMemoryKeyCount('user_age:'),
            chatUserEntries: this.getMemoryKeyCount('chat_user:'),
          }
        };
      }
      return { type: 'None', totalKeys: 0 };
    } catch (error) {
      console.error('캐시 통계 조회 실패:', error);
      return { type: 'Error', totalKeys: 0 };
    }
  }

  async getKeyCount(pattern) {
    try {
      if (this.client) {
        const keys = await this.client.keys(pattern);
        return keys.length;
      }
      return 0;
    } catch (error) {
      return 0;
    }
  }

  getMemoryKeyCount(prefix) {
    let count = 0;
    for (const [key] of this.memoryCache) {
      if (key.startsWith(prefix)) count++;
    }
    return count;
  }

  extractHitRate(info) {
    const hits = info.match(/keyspace_hits:(\d+)/)?.[1] || 0;
    const misses = info.match(/keyspace_misses:(\d+)/)?.[1] || 0;
    return hits + misses > 0
      ? ((hits / (parseInt(hits) + parseInt(misses))) * 100).toFixed(1) + "%"
      : "0%";
  }

  extractKeyCount(keyspace) {
    return keyspace.match(/keys=(\d+)/)?.[1] || 0;
  }

  extractMemoryUsage(info) {
    return info.match(/used_memory_human:([^\r\n]+)/)?.[1] || "N/A";
  }

  // 🔍 개발자 페이지 검색 결과 캐싱
  async cacheDeveloperSearch(searchQuery, page, limit, results) {
    const key = `dev_search:${searchQuery || 'all'}:${page}:${limit}`;
    const data = {
      searchQuery,
      page,
      limit,
      results,
      cachedAt: new Date().toISOString(),
      totalResults: results.total,
      users: results.results
    };
    
    // 검색 결과는 10분 캐시 (자주 변하지 않음)
    await this.setCache(key, data, 600);
    
    // 저장 위치 명시
    const cacheType = this.client ? 'Redis' : 'Memory';
    console.log(`🔍 개발자 검색 결과 캐싱 [${cacheType}]: "${searchQuery || 'all'}" 페이지 ${page} (${results.results?.length}명)`);
  }

  async getCachedDeveloperSearch(searchQuery, page, limit) {
    const key = `dev_search:${searchQuery || 'all'}:${page}:${limit}`;
    const cached = await this.getCache(key);
    
    if (cached) {
      const cacheType = this.client ? 'Redis' : 'Memory';
      console.log(`✅ 캐시된 검색 결과 사용 [${cacheType}]: "${searchQuery || 'all'}" 페이지 ${page}`);
      return cached;
    }
    
    return null;
  }

  // 🗑️ 개발자 페이지 캐시 무효화 (사용자 정보 변경 시)
  async invalidateDeveloperCache() {
    try {
      const cacheType = this.client ? 'Redis' : 'Memory';
      console.log(`🗑️ 개발자 캐시 무효화 시도 [${cacheType}]...`);
      
      if (this.client) {
        // Redis에서 패턴 매칭으로 삭제
        const searchKeys = await this.client.keys('dev_search:*');
        
        if (searchKeys.length > 0) {
          await this.client.del(searchKeys);
          console.log(`🗑️ [Redis] 개발자 캐시 무효화: ${searchKeys.length}개 키 삭제`);
        } else {
          console.log(`🗑️ [Redis] 삭제할 개발자 캐시 없음`);
        }
      } else if (this.memoryCache) {
        // 메모리 캐시에서 패턴 매칭으로 삭제
        let deletedCount = 0;
        for (const [key] of this.memoryCache) {
          if (key.startsWith('dev_search:')) {
            this.memoryCache.delete(key);
            deletedCount++;
          }
        }
        console.log(`🗑️ [Memory] 개발자 캐시 무효화: ${deletedCount}개 키 삭제`);
      }
    } catch (error) {
      console.error('개발자 캐시 무효화 실패:', error);
    }
  }

  // 📊 개발자 캐시 통계
  async getDeveloperCacheStats() {
    try {
      const stats = {
        searchCacheCount: 0,
        totalDeveloperCacheSize: 0
      };
      
      if (this.client) {
        const searchKeys = await this.client.keys('dev_search:*');
        stats.searchCacheCount = searchKeys.length;
        stats.totalDeveloperCacheSize = searchKeys.length;
      } else if (this.memoryCache) {
        for (const [key] of this.memoryCache) {
          if (key.startsWith('dev_search:')) stats.searchCacheCount++;
        }
        stats.totalDeveloperCacheSize = stats.searchCacheCount;
      }
      
      return stats;
    } catch (error) {
      console.error('개발자 캐시 통계 조회 실패:', error);
      return { searchCacheCount: 0, totalDeveloperCacheSize: 0 };
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // 🎯 사용자 정적 정보 캐싱 (새로 추가)
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  /**
   * 사용자 정적 정보 조회 (변하지 않는 데이터)
   *
   * 캐시 대상:
   * - nickname, profilePhoto, gender, star (거의 변하지 않음)
   * - numOfChat, chatTimer (실시간 계산의 기준점)
   *
   * TTL: 30분 (1800초)
   *
   * @param {string} userId - 사용자 ID
   * @returns {Object|null} 캐시된 사용자 정보 또는 null
   */
  async getUserStaticInfo(userId) {
    try {
      const key = `user_static:${userId}`;

      // Redis 클라이언트가 있으면 Redis에서 조회
      if (this.client) {
        const cached = await this.client.get(key);
        if (cached) {
          console.log(`💾 [Redis HIT] 정적 정보: ${userId}`);
          return JSON.parse(cached);
        }
      } else {
        // ✅ 수정: expires 체크 후 반환
        const data = this.memoryCache.get(key);
        if (data) {
          // TTL 만료 확인
          if (data.expires > Date.now()) {
            console.log(`💾 [Memory HIT] 정적 정보: ${userId}`);
            return JSON.parse(data.value); // ✅ value 필드에서 파싱
          } else {
            // 만료된 캐시 삭제
            this.memoryCache.delete(key);
            console.log(`🗑️ [Memory 만료] 정적 정보: ${userId}`);
          }
        }
      }

      console.log(`❌ [Cache MISS] 정적 정보: ${userId}`);
      return null;
    } catch (error) {
      console.error(`⚠️ 정적 정보 캐시 조회 실패 (${userId}):`, error.message);
      return null;
    }
  }

  /**
   * 사용자 정적 정보 저장
   *
   * @param {string} userId - 사용자 ID
   * @param {Object} userData - 저장할 사용자 정보
   * @param {number} ttl - TTL (초 단위, 기본값: 1800초 = 30분)
   */
  async cacheUserStaticInfo(userId, userData, ttl = 1800) {
    try {
      const key = `user_static:${userId}`;

      // Redis 클라이언트가 있으면 Redis에 저장
      if (this.client) {
        await this.client.setEx(key, ttl, JSON.stringify(userData));
        console.log(`✅ [Redis 캐싱] 정적 정보: ${userId} (TTL: ${ttl}초)`);
      } else {
        // ✅ 수정: expires 필드 포함하여 저장
        this.memoryCache.set(key, {
          value: JSON.stringify(userData),
          expires: Date.now() + (ttl * 1000)
        });
        console.log(`✅ [Memory 캐싱] 정적 정보: ${userId} (TTL: ${ttl}초)`);
      }
    } catch (error) {
      console.error(`⚠️ 정적 정보 캐싱 실패 (${userId}):`, error.message);
      // 캐싱 실패해도 애플리케이션은 정상 작동
    }
  }

  /**
   * 사용자 정적 정보 캐시 무효화
   *
   * 사용 시점:
   * - 채팅 충전 업데이트 후
   * - 프로필 정보 수정 후
   * - 사용자 정보가 변경된 모든 경우
   *
   * @param {string} userId - 사용자 ID
   */
  async invalidateUserStaticInfo(userId) {
    try {
      const key = `user_static:${userId}`;

      // Redis 클라이언트가 있으면 Redis에서 삭제
      if (this.client) {
        await this.client.del(key);
        console.log(`🗑️ [Redis 무효화] 정적 정보: ${userId}`);
      } else {
        // Redis 없으면 메모리 캐시에서 삭제
        this.memoryCache.delete(key);
        console.log(`🗑️ [Memory 무효화] 정적 정보: ${userId}`);
      }
    } catch (error) {
      console.error(`⚠️ 정적 정보 캐시 무효화 실패 (${userId}):`, error.message);
      // 무효화 실패해도 다음 TTL 만료 시 자동 삭제됨
    }
  }
  startMemoryCleanup() {
    const MAX_MEMORY_ENTRIES = 10000; // 메모리 캐시 최대 항목 수

    this.memoryCleanupInterval = setInterval(() => {
      if (!this.client && this.memoryCache && this.memoryCache.size > 0) {
        const now = Date.now();
        let cleaned = 0;
        let total = this.memoryCache.size;

        // 1. 만료된 항목 정리
        for (const [key, value] of this.memoryCache.entries()) {
          if (value.expires && value.expires < now) {
            this.memoryCache.delete(key);
            cleaned++;
          }
        }

        // 2. TTL=0 (무제한) 항목으로 인한 메모리 누수 방지
        if (this.memoryCache.size > MAX_MEMORY_ENTRIES) {
          const overflow = this.memoryCache.size - MAX_MEMORY_ENTRIES;
          let removed = 0;
          // Map은 삽입 순서를 유지하므로, 가장 오래된 무제한 항목부터 제거
          for (const [key, value] of this.memoryCache.entries()) {
            if (removed >= overflow) break;
            if (value.expires === Infinity) {
              this.memoryCache.delete(key);
              removed++;
              cleaned++;
            }
          }
          if (removed > 0) {
            console.log(`⚠️ [메모리 캐시] 최대 크기 초과 - 무제한 항목 ${removed}개 정리`);
          }
        }

        if (cleaned > 0) {
          console.log(`🧹 [메모리 캐시 정리] ${cleaned}/${total}개 항목 삭제, 남은 항목: ${this.memoryCache.size}개`);
        }
      }
    }, 5 * 60 * 1000);

    console.log('✅ 메모리 캐시 자동 정리 시작 (5분 간격)');
  }

  stopMemoryCleanup() {
    if (this.memoryCleanupInterval) {
      clearInterval(this.memoryCleanupInterval);
      this.memoryCleanupInterval = null;
      console.log('🛑 메모리 캐시 자동 정리 중지');
    }
  }

  getMemoryCacheStats() {
    if (!this.memoryCache) {
      return { total: 0, expired: 0 };
    }

    const now = Date.now();
    let total = 0;
    let expired = 0;

    for (const [key, value] of this.memoryCache.entries()) {
      total++;
      if (value.expires && value.expires < now) {
        expired++;
      }
    }

    return { total, expired, active: total - expired };
  }

  /**
   * 패턴 매칭 캐시 일괄 삭제 (선택 사항)
   *
   * @param {string} pattern - 삭제할 캐시 키 패턴 (예: 'user_*')
   * @returns {Promise<number>} 삭제된 키 개수
   */
  async deleteCacheByPattern(pattern) {
    if (!this.isConnected) return 0;

    try {
      if (this.client) {
        // Redis SCAN으로 패턴 매칭 키 찾기
        const keys = [];
        let cursor = 0;

        do {
          const reply = await this.client.scan(cursor, {
            MATCH: pattern,
            COUNT: 100
          });
          cursor = reply.cursor;
          keys.push(...reply.keys);
        } while (cursor !== 0);

        if (keys.length > 0) {
          await this.client.del(keys);
          console.log(`🗑️ [패턴 캐시 삭제] ${pattern}: ${keys.length}개`);
        }

        return keys.length;
      } else if (this.memoryCache) {
        // 메모리 캐시에서 패턴 매칭
        let count = 0;
        const regex = new RegExp(pattern.replace('*', '.*'));

        for (const key of this.memoryCache.keys()) {
          if (regex.test(key)) {
            this.memoryCache.delete(key);
            count++;
          }
        }

        console.log(`🗑️ [메모리 패턴 캐시 삭제] ${pattern}: ${count}개`);
        return count;
      }

      return 0;
    } catch (error) {
      console.error(`❌ [패턴 캐시 삭제 실패] ${pattern}:`, error.message);
      return 0;
    }
  }


  /**
   * 특정 필드만 선택적으로 무효화
   * @param {string} userId - 사용자 ID
   * @param {string} field - 무효화할 필드명 (예: 'star')
   */
  async invalidateUserField(userId, field) {
    try {
      const cacheKey = `user_${userId}_${field}`;

      if (this.client && this.isConnected) {
        // Redis: star 필드만 무효화
        await this.client.del(cacheKey);
        console.log(`🗑️ [Redis 선택적 무효화] ${cacheKey}`);
      } else {
        // Memory: star 필드만 삭제
        this.memoryCache.delete(cacheKey);
        console.log(`🗑️ [Memory 선택적 무효화] ${cacheKey}`);
      }
    } catch (error) {
      console.error(`❌ 필드 무효화 실패 (${userId}.${field}):`, error);
    }
  }

  /**
   * star 값 캐싱 (5분 TTL)
   * @param {string} userId - 사용자 ID
   * @param {number} starValue - 별점 값
   * @param {number} ttl - Time To Live (초 단위, 기본 300초=5분)
   */
  async cacheUserStar(userId, starValue, ttl = 300) {
    const cacheKey = `user_${userId}_star`;

    try {
      if (this.client && this.isConnected) {
        // Redis 캐싱
        await this.client.setEx(
            cacheKey,
            ttl,
            JSON.stringify({ star: starValue, cachedAt: new Date() })
        );
        console.log(`✅ [Redis 캐싱] ${cacheKey} = ${starValue} (TTL: ${ttl}초)`);
      } else {
        // Memory 캐싱
        this.memoryCache.set(cacheKey, {
          star: starValue,
          cachedAt: new Date()
        });

        // TTL 후 자동 삭제
        setTimeout(() => {
          this.memoryCache.delete(cacheKey);
          console.log(`⏰ [Memory TTL 만료] ${cacheKey}`);
        }, ttl * 1000);

        console.log(`✅ [Memory 캐싱] ${cacheKey} = ${starValue} (TTL: ${ttl}초)`);
      }
    } catch (error) {
      console.error(`❌ star 캐싱 실패 (${userId}):`, error);
    }
  }

  /**
   * star 값 조회 (캐시에서)
   * @param {string} userId - 사용자 ID
   * @returns {Promise<number|null>} - 캐시된 star 값 또는 null
   */
  async getCachedUserStar(userId) {
    const cacheKey = `user_${userId}_star`;

    try {
      if (this.client && this.isConnected) {
        // Redis에서 조회
        const cached = await this.client.get(cacheKey);
        if (cached) {
          const parsed = JSON.parse(cached);
          console.log(`💾 [Redis HIT] ${cacheKey} = ${parsed.star}`);
          return parsed.star;
        }
        console.log(`🔍 [Redis MISS] ${cacheKey}`);
        return null;
      } else {
        // Memory에서 조회
        const cached = this.memoryCache.get(cacheKey);
        if (cached) {
          console.log(`💾 [Memory HIT] ${cacheKey} = ${cached.star}`);
          return cached.star;
        }
        console.log(`🔍 [Memory MISS] ${cacheKey}`);
        return null;
      }
    } catch (error) {
      console.error(`❌ star 조회 실패 (${userId}):`, error);
      return null;
    }
  }

  async invalidateUserFriends(userId) {
    try {
      await Promise.all([
        this.deleteCache(`auth_user_${userId}`),
        this.deleteCache(`user_friends_ids_${userId}`),
        this.deleteCache(`user_profile_full_${userId}`)
      ]);

      const cacheType = this.client ? 'Redis' : 'Memory';
      console.log(`✅ [${cacheType}] 선택적 무효화 - 친구 목록: ${userId}`);
    } catch (error) {
      console.error(`❌ 친구 캐시 무효화 실패 (${userId}):`, error);
    }
  }

  async invalidateFriendDeletion(userId1, userId2) {
    try {
      console.log(`🗑️ [친구 삭제 캐시] 무효화 시작: ${userId1} ↔ ${userId2}`);

      await Promise.all([
        // 사용자 1의 모든 관련 캐시
        this.invalidateUserFriends(userId1),
        this.invalidateUserCache(userId1),

        // 사용자 2의 모든 관련 캐시
        this.invalidateUserFriends(userId2),
        this.invalidateUserCache(userId2)
      ]);

      const cacheType = this.client ? 'Redis' : 'Memory';
      console.log(`✅ [${cacheType}] 친구 삭제 캐시 무효화 완료`);
    } catch (error) {
      console.error(`❌ 친구 삭제 캐시 무효화 실패:`, error);
      throw error;
    }
  }

  /**
   * 특정 사용자 필드 값 캐싱 (범용)
   * @param {string} userId - 사용자 ID
   * @param {string} field - 필드명 (예: 'numOfChat', 'star')
   * @param {any} value - 캐싱할 값
   * @param {number} ttl - Time To Live (초 단위, 기본 60초)
   */
  async cacheUserField(userId, field, value, ttl = 60) {
    const cacheKey = `user_${userId}_${field}`;

    try {
      if (this.client && this.isConnected) {
        // Redis 캐싱
        await this.client.setEx(
            cacheKey,
            ttl,
            JSON.stringify({ [field]: value, cachedAt: new Date() })
        );
        console.log(`✅ [Redis 캐싱] ${cacheKey} = ${value} (TTL: ${ttl}초)`);
      } else {
        // Memory 캐싱
        this.memoryCache.set(cacheKey, {
          value: JSON.stringify({ [field]: value, cachedAt: new Date() }),
          expires: Date.now() + (ttl * 1000)
        });

        console.log(`✅ [Memory 캐싱] ${cacheKey} = ${value} (TTL: ${ttl}초)`);
      }
    } catch (error) {
      console.error(`❌ ${field} 캐싱 실패 (${userId}):`, error);
    }
  }

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // 🎯 사용자 닉네임 캐싱 (채팅 최적화용)
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  /**
   * 사용자 닉네임 조회 (캐시 우선)
   *
   * 사용처:
   * - socketIO.js의 sendMessage 이벤트
   * - 메시지 전송 시 sender 닉네임 표시
   *
   * @param {string} userId - 사용자 ID
   * @returns {Promise<string|null>} 캐시된 닉네임 또는 null
   */
  async getUserNickname(userId) {
    const key = `user:nickname:${userId}`;

    try {
      if (this.client && this.isConnected) {
        // Redis에서 조회
        const cached = await this.client.get(key);
        if (cached) {
          console.log(`💾 [Redis HIT] 닉네임: ${userId} → "${cached}"`);
          return cached; // 문자열 그대로 반환 (JSON 파싱 불필요)
        }
        console.log(`🔍 [Redis MISS] 닉네임: ${userId}`);
        return null;
      } else {
        // Memory에서 조회
        const data = this.memoryCache.get(key);
        if (data && data.expires > Date.now()) {
          console.log(`💾 [Memory HIT] 닉네임: ${userId} → "${data.value}"`);
          return data.value; // expires 체크 후 value 반환
        }
        console.log(`🔍 [Memory MISS] 닉네임: ${userId}`);
        return null;
      }
    } catch (error) {
      console.error(`❌ 닉네임 캐시 조회 실패 (${userId}):`, error);
      return null;
    }
  }

  /**
   * 사용자 닉네임 저장 (30분 TTL)
   *
   * 사용처:
   * - socketIO.js (DB 조회 후 캐싱)
   *
   * @param {string} userId - 사용자 ID
   * @param {string} nickname - 닉네임
   * @param {number} ttl - Time To Live (초 단위, 기본 1800초=30분)
   */
  async cacheUserNickname(userId, nickname, ttl = 1800) {
    const key = `user:nickname:${userId}`;

    try {
      if (this.client && this.isConnected) {
        // Redis에 저장 (문자열 그대로)
        await this.client.setEx(key, ttl, nickname);
        console.log(`✅ [Redis 캐싱] 닉네임: ${userId} → "${nickname}" (TTL: ${ttl}초)`);
      } else {
        // Memory에 저장
        this.memoryCache.set(key, {
          value: nickname, // 문자열 그대로 저장
          expires: Date.now() + (ttl * 1000)
        });
        console.log(`✅ [Memory 캐싱] 닉네임: ${userId} → "${nickname}" (TTL: ${ttl}초)`);
      }
    } catch (error) {
      console.error(`❌ 닉네임 캐싱 실패 (${userId}):`, error);
    }
  }

  /**
   * 사용자 닉네임 캐시 무효화
   *
   * 사용 시점:
   * - 닉네임 변경 시 (userService.js)
   * - 프로필 업데이트 시
   *
   * @param {string} userId - 사용자 ID
   */
  async invalidateUserNickname(userId) {
    const key = `user:nickname:${userId}`;

    try {
      if (this.client && this.isConnected) {
        await this.client.del(key);
        console.log(`🗑️ [Redis 무효화] 닉네임: ${userId}`);
      } else {
        this.memoryCache.delete(key);
        console.log(`🗑️ [Memory 무효화] 닉네임: ${userId}`);
      }
    } catch (error) {
      console.error(`❌ 닉네임 캐시 무효화 실패 (${userId}):`, error);
    }
  }


  /**
   * 특정 사용자 필드 값 조회 (캐시에서)
   * @param {string} userId - 사용자 ID
   * @param {string} field - 필드명
   * @returns {Promise<any|null>} - 캐시된 값 또는 null
   */
  async getCachedUserField(userId, field) {
    const cacheKey = `user_${userId}_${field}`;

    try {
      if (this.client && this.isConnected) {
        // Redis에서 조회
        const cached = await this.client.get(cacheKey);
        if (cached) {
          const parsed = JSON.parse(cached);
          console.log(`💾 [Redis HIT] ${cacheKey} = ${parsed[field]}`);
          return parsed[field];
        }
        console.log(`🔍 [Redis MISS] ${cacheKey}`);
        return null;
      } else {
        // Memory에서 조회
        const cached = this.memoryCache.get(cacheKey);
        if (cached && cached.expires > Date.now()) {
          const parsed = JSON.parse(cached.value);
          console.log(`💾 [Memory HIT] ${cacheKey} = ${parsed[field]}`);
          return parsed[field];
        }
        console.log(`🔍 [Memory MISS] ${cacheKey}`);
        return null;
      }
    } catch (error) {
      console.error(`❌ ${field} 조회 실패 (${userId}):`, error);
      return null;
    }
  }

  /**
   * 친구방 생성 시 캐시 무효화
   */
  async invalidateFriendRoomCache(userId, friendId) {
    try {
      const keys = [
        `user_blocks_${userId}`,
        `users_blocked_me_${userId}`,
        `user_blocks_${friendId}`,
        `users_blocked_me_${friendId}`
      ];

      for (const key of keys) {
        await this.deleteCache(key);
      }

      console.log(`✅ 친구방 캐시 무효화: ${userId} ↔ ${friendId}`);
    } catch (error) {
      console.error(`❌ 캐시 무효화 실패:`, error);
      // 실패해도 TTL로 자동 복구되므로 throw 안함
    }
  }

  /**
   * 친구방 ID 캐싱 조회
   * @param {string} userId1 - 사용자 1 ID
   * @param {string} userId2 - 사용자 2 ID
   * @returns {Promise<string|null>} 캐시된 방 ID 또는 null
   */
  async getCachedFriendRoomId(userId1, userId2) {
    try {
      const sortedIds = [userId1, userId2].map(id => id.toString()).sort();
      const key = `friend_room:${sortedIds[0]}:${sortedIds[1]}`;

      const roomId = await this.getCache(key);

      if (roomId) {
        console.log(`✅ [캐시 HIT] 친구방 ID: ${roomId}`);
        return roomId;
      }

      console.log(`❌ [캐시 MISS] 친구방: ${sortedIds[0]} ↔ ${sortedIds[1]}`);
      return null;
    } catch (error) {
      console.error('❌ 친구방 ID 캐시 조회 실패:', error);
      return null;
    }
  }

  /**
   * 친구방 ID 저장 (영구 캐싱)
   * @param {string} userId1 - 사용자 1 ID
   * @param {string} userId2 - 사용자 2 ID
   * @param {string} roomId - 방 ID
   */
  async cacheFriendRoomId(userId1, userId2, roomId) {
    try {
      const sortedIds = [userId1, userId2].map(id => id.toString()).sort();
      const key = `friend_room:${sortedIds[0]}:${sortedIds[1]}`;

      // TTL 0 = 무제한 (친구 삭제 전까지 유지)
      await this.setCache(key, roomId, 0);

      console.log(`💾 [캐싱] 친구방 ID: ${key} → ${roomId}`);
    } catch (error) {
      console.error('❌ 친구방 ID 캐싱 실패:', error);
    }
  }

  /**
   * 친구방 ID 캐시 무효화 (친구 삭제/차단 시)
   * @param {string} userId1 - 사용자 1 ID
   * @param {string} userId2 - 사용자 2 ID
   */
  async invalidateFriendRoomId(userId1, userId2) {
    try {
      const sortedIds = [userId1, userId2].map(id => id.toString()).sort();
      const key = `friend_room:${sortedIds[0]}:${sortedIds[1]}`;

      await this.deleteCache(key);

      console.log(`🗑️ [무효화] 친구방 ID: ${key}`);
    } catch (error) {
      console.error('❌ 친구방 ID 캐시 무효화 실패:', error);
    }
  }





}




export default new IntelligentCache();