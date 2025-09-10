import Redis from "redis";
import ComprehensiveEncryption from "../encryption/comprehensiveEncryption.js";

class IntelligentCache {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.memoryCache = new Map();
    this.initializeRedis(); // 비동기 초기화 시작
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
        await this.client.setEx(key, ttl, value);
      } else if (this.memoryCache) {
        this.memoryCache.set(key, { value, expires: Date.now() + (ttl * 1000) });
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

  // 복호화된 사용자 정보 캐싱 (관리자용)
  async cacheDecryptedUser(userId, decryptedUserData) {
    const key = `decrypted_user:${userId}`;
    await this.setCache(key, decryptedUserData, 1800); // 30분 TTL
  }

  async getDecryptedUser(userId) {
    const key = `decrypted_user:${userId}`;
    return await this.getCache(key);
  }

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
    
    const cacheAge = Date.now() - new Date(data.calculatedAt).getTime();
    
    // 24시간 이상 지났으면 무효화
    if (cacheAge > 86400000) {
      await this.deleteCache(`user_age:${userId}`);
      return null;
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
    try {
      if (this.client) {
        await this.client.del(key);
      } else if (this.memoryCache) {
        this.memoryCache.delete(key);
      }
    } catch (error) {
      console.error('캐시 삭제 실패:', error);
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
}

export default new IntelligentCache();