// 🏢 MAGI Pixel Office — app.js
// Firebase 리스너 + 캐릭터 렌더링 + 말풍선 + 반응형

(function () {
  'use strict';

  // ===== 상수 =====
  const AWAY_TIMEOUT_MS = 30 * 60 * 1000; // 30분 무응답 → away
  const DB_PATH = 'team_magi';

  // ===== 상태 =====
  let characters = [];
  let firebaseReady = false;
  let dbRef = null;
  let isMobile = window.innerWidth <= 767;

  // ===== 초기화 =====
  async function init() {
    try {
      // characters.json 로드
      const res = await fetch('config/characters.json');
      const config = await res.json();
      characters = config.members;

      // Firebase 초기화
      firebase.initializeApp(firebaseConfig);
      const db = firebase.database();
      dbRef = db.ref(DB_PATH);

      // 연결 상태 모니터링
      setupConnectionMonitor(db);

      // 화면 렌더링
      renderOffice();
      if (isMobile) renderMobileGrid();

      // Firebase 리스너
      setupFirebaseListeners();

      // 모바일 배터리 절약: visibilitychange
      setupVisibilityHandler();

      // 반응형 감지
      window.addEventListener('resize', handleResize);

      // 모달 닫기
      document.getElementById('modal-close').addEventListener('click', closeModal);
      document.getElementById('modal-overlay').addEventListener('click', function (e) {
        if (e.target === this) closeModal();
      });

    } catch (err) {
      console.error('초기화 실패:', err);
      showError('초기화에 실패했습니다: ' + err.message);
    }
  }

  // ===== Firebase 연결 상태 모니터링 =====
  function setupConnectionMonitor(db) {
    const connRef = db.ref('.info/connected');
    const dot = document.getElementById('status-dot');
    const text = document.getElementById('status-text');

    connRef.on('value', function (snap) {
      if (snap.val() === true) {
        dot.className = 'connected';
        text.textContent = '실시간 연결됨';
        hideError();
        firebaseReady = true;
      } else {
        dot.className = 'disconnected';
        text.textContent = '연결 끊김';
        if (firebaseReady) {
          showError('연결이 끊겼습니다. 마지막 상태를 표시합니다.');
        }
      }
    });
  }

  // ===== 사무실 렌더링 (데스크톱) =====
  function renderOffice() {
    const office = document.getElementById('office');

    characters.forEach(function (char) {
      var wrapper = document.createElement('div');
      wrapper.className = 'character-wrapper';
      wrapper.id = 'char-' + char.id;
      wrapper.style.left = (char.position.x - 32) + 'px';
      wrapper.style.top = (char.position.y - 32) + 'px';
      wrapper.style.setProperty('--char-color', char.color);
      wrapper.style.setProperty('--char-color-glow', char.color);

      // 말풍선
      var bubble = document.createElement('div');
      bubble.className = 'speech-bubble';
      bubble.id = 'bubble-' + char.id;
      wrapper.appendChild(bubble);

      // 아바타 (Phase 1: 컬러 원형 + 이름 첫 글자)
      var avatar = document.createElement('div');
      avatar.className = 'character-avatar status-idle';
      avatar.id = 'avatar-' + char.id;
      avatar.style.background = 'radial-gradient(circle at 35% 35%, ' +
        lightenColor(char.color, 30) + ', ' + char.color + ')';
      avatar.textContent = char.name[0];
      wrapper.appendChild(avatar);

      // 이름표
      var name = document.createElement('div');
      name.className = 'character-name';
      name.textContent = char.name;
      wrapper.appendChild(name);

      // 역할
      var role = document.createElement('div');
      role.className = 'character-role';
      role.textContent = char.role;
      wrapper.appendChild(role);

      // 책상
      var desk = document.createElement('div');
      desk.className = 'desk' + (char.desk === 'large' ? ' large' : '');
      wrapper.appendChild(desk);

      // 클릭 → 모달 (모바일 대응)
      wrapper.addEventListener('click', function () {
        openModal(char.id);
      });

      office.appendChild(wrapper);
    });
  }

  // ===== 모바일 그리드 렌더링 =====
  function renderMobileGrid() {
    // 기존 그리드 제거
    var existing = document.getElementById('mobile-grid');
    if (existing) existing.remove();

    var container = document.getElementById('office-container');
    var grid = document.createElement('div');
    grid.id = 'mobile-grid';

    characters.forEach(function (char) {
      var card = document.createElement('div');
      card.className = 'mobile-card';
      card.id = 'mobile-' + char.id;

      card.innerHTML =
        '<div class="card-avatar" style="background: radial-gradient(circle at 35% 35%, ' +
        lightenColor(char.color, 30) + ', ' + char.color + ')">' + char.name[0] + '</div>' +
        '<div class="card-name">' +
        '<span class="card-status-dot" id="mobile-dot-' + char.id + '" style="background: #888"></span>' +
        char.name + '</div>' +
        '<div class="card-role">' + char.role + '</div>' +
        '<div class="card-task" id="mobile-task-' + char.id + '">대기 중</div>';

      card.addEventListener('click', function () {
        openModal(char.id);
      });

      grid.appendChild(card);
    });

    container.appendChild(grid);
  }

  // ===== Firebase 리스너 =====
  function setupFirebaseListeners() {
    dbRef.on('value', function (snapshot) {
      var data = snapshot.val();
      if (!data) return;

      characters.forEach(function (char) {
        var state = data[char.id];
        if (state) {
          updateCharacter(char.id, state);
        }
      });
    });
  }

  // ===== 캐릭터 상태 업데이트 =====
  function updateCharacter(id, state) {
    var now = Date.now();
    var status = state.status || 'offline';
    var mood = state.mood || 'idle';
    var task = state.task || '';
    var updatedAt = state.updatedAt || 0;

    // 30분 이상 업데이트 없으면 자동 away
    if (updatedAt > 0 && (now - updatedAt) > AWAY_TIMEOUT_MS) {
      status = 'away';
      mood = 'idle';
    }

    // 데스크톱: 아바타 업데이트
    var avatar = document.getElementById('avatar-' + id);
    if (avatar) {
      // 기존 상태/무드 클래스 제거
      avatar.className = 'character-avatar';
      avatar.classList.add('status-' + status);
      if (mood !== 'idle') {
        avatar.classList.add('mood-' + mood);
      }
    }

    // 데스크톱: 말풍선 업데이트
    var bubble = document.getElementById('bubble-' + id);
    if (bubble) {
      bubble.className = 'speech-bubble';
      if (task && status !== 'offline' && !(status === 'idle' && mood === 'idle')) {
        bubble.textContent = task;
        bubble.title = task; // 호버 시 전체 텍스트
        bubble.classList.add('visible');
        if (mood === 'alert') bubble.classList.add('alert');
        if (status === 'away') bubble.classList.add('away');
      } else {
        bubble.textContent = '';
        // 말풍선 숨김 (idle + 대기)
      }
    }

    // 모바일: 카드 업데이트
    var mobileDot = document.getElementById('mobile-dot-' + id);
    if (mobileDot) {
      var dotColors = {
        active: '#32CD32',
        idle: '#FFD700',
        away: '#888',
        offline: '#444'
      };
      mobileDot.style.background = dotColors[status] || '#444';
    }

    var mobileTask = document.getElementById('mobile-task-' + id);
    if (mobileTask) {
      mobileTask.textContent = task || '대기 중';
    }

    // 모달이 열려있고 해당 캐릭터면 업데이트
    var modalOverlay = document.getElementById('modal-overlay');
    if (!modalOverlay.classList.contains('hidden') && modalOverlay.dataset.charId === id) {
      fillModal(id, state);
    }
  }

  // ===== 모달 =====
  function openModal(charId) {
    var char = characters.find(function (c) { return c.id === charId; });
    if (!char) return;

    var overlay = document.getElementById('modal-overlay');
    overlay.dataset.charId = charId;
    overlay.classList.remove('hidden');

    // Firebase에서 최신 데이터 가져오기
    dbRef.child(charId).once('value', function (snap) {
      var state = snap.val() || { status: 'offline', mood: 'idle', task: '', updatedAt: 0 };
      fillModal(charId, state);
    });
  }

  function fillModal(charId, state) {
    var char = characters.find(function (c) { return c.id === charId; });
    if (!char) return;

    var modalAvatar = document.getElementById('modal-avatar');
    modalAvatar.style.background = 'radial-gradient(circle at 35% 35%, ' +
      lightenColor(char.color, 30) + ', ' + char.color + ')';

    document.getElementById('modal-name').textContent = char.name;
    document.getElementById('modal-role').textContent = char.role;

    var statusEl = document.getElementById('modal-status');
    var statusLabels = {
      active: '🟢 활동 중',
      idle: '🟡 대기',
      away: '⚪ 자리비움',
      offline: '⚫ 오프라인'
    };
    statusEl.textContent = statusLabels[state.status] || '⚫ 오프라인';
    statusEl.style.background = '#2a2a4a';

    document.getElementById('modal-task').textContent = state.task || '작업 없음';

    var updatedAt = state.updatedAt;
    if (updatedAt) {
      var date = new Date(updatedAt);
      document.getElementById('modal-updated').textContent =
        '마지막 업데이트: ' + date.toLocaleString('ko-KR');
    } else {
      document.getElementById('modal-updated').textContent = '';
    }
  }

  function closeModal() {
    document.getElementById('modal-overlay').classList.add('hidden');
  }

  // ===== visibilitychange (모바일 배터리 절약) =====
  function setupVisibilityHandler() {
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) {
        // 백그라운드: Firebase 리스너 해제
        if (dbRef) dbRef.off();
      } else {
        // 포그라운드: 리스너 재연결
        if (dbRef) setupFirebaseListeners();
      }
    });
  }

  // ===== 반응형 =====
  function handleResize() {
    var wasMobile = isMobile;
    isMobile = window.innerWidth <= 767;

    if (isMobile && !wasMobile) {
      renderMobileGrid();
    } else if (!isMobile && wasMobile) {
      var grid = document.getElementById('mobile-grid');
      if (grid) grid.remove();
    }
  }

  // ===== 에러 표시 =====
  function showError(msg) {
    var banner = document.getElementById('error-banner');
    document.getElementById('error-message').textContent = msg;
    banner.classList.remove('hidden');
  }

  function hideError() {
    document.getElementById('error-banner').classList.add('hidden');
  }

  // ===== 유틸: 색상 밝게 =====
  function lightenColor(hex, percent) {
    hex = hex.replace('#', '');
    var r = parseInt(hex.substring(0, 2), 16);
    var g = parseInt(hex.substring(2, 4), 16);
    var b = parseInt(hex.substring(4, 6), 16);
    r = Math.min(255, Math.floor(r + (255 - r) * percent / 100));
    g = Math.min(255, Math.floor(g + (255 - g) * percent / 100));
    b = Math.min(255, Math.floor(b + (255 - b) * percent / 100));
    return '#' + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
  }

  // ===== 브라우저 오프라인 감지 =====
  window.addEventListener('offline', function () {
    showError('오프라인 상태입니다. 인터넷 연결을 확인해주세요.');
    document.getElementById('status-dot').className = 'disconnected';
    document.getElementById('status-text').textContent = '오프라인';
  });

  window.addEventListener('online', function () {
    hideError();
    document.getElementById('status-text').textContent = '재연결 중...';
  });

  // ===== 시작! =====
  document.addEventListener('DOMContentLoaded', init);
})();
