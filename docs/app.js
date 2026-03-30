// 🏢 MAGI Pixel Office — app.js
// Phase 4: 알림 + 사운드 + 채팅 + 테마 추가

(function () {
  'use strict';

  // ===== 상수 =====
  const AWAY_TIMEOUT_MS = 30 * 60 * 1000;
  const DB_PATH = 'team_magi';
  const EVENTS_PATH = 'team_magi_events';
  const CHAT_PATH = 'team_magi_chat';
  const MAX_EVENTS = 50;
  const MAX_CHAT = 100;

  // ===== 상태 =====
  let characters = [];
  let firebaseReady = false;
  let dbRef = null;
  let eventsRef = null;
  let chatRef = null;
  let eventCount = 0;
  let chatCount = 0;
  let isMobile = window.innerWidth <= 767;
  let notifyEnabled = false;
  let soundEnabled = true;
  let audioCtx = null;

  // ===== 초기화 =====
  async function init() {
    try {
      const res = await fetch('config/characters.json');
      const config = await res.json();
      characters = config.members;

      firebase.initializeApp(firebaseConfig);
      const db = firebase.database();
      dbRef = db.ref(DB_PATH);
      eventsRef = db.ref(EVENTS_PATH);
      chatRef = db.ref(CHAT_PATH);

      await firebase.auth().signInAnonymously();

      setupConnectionMonitor(db);
      renderOffice();
      if (isMobile) renderMobileGrid();
      setupFirebaseListeners();
      setupEventListeners();
      setupChatListeners();
      setupVisibilityHandler();

      window.addEventListener('resize', handleResize);

      document.getElementById('modal-close').addEventListener('click', closeModal);
      document.getElementById('modal-overlay').addEventListener('click', function (e) {
        if (e.target === this) closeModal();
      });

      // Phase 4: 버튼 이벤트
      setupNotifyButton();
      setupSoundButton();
      setupThemeButton();
      setupChatInput();

    } catch (err) {
      console.error('초기화 실패:', err);
      showError('초기화에 실패했습니다: ' + err.message);
    }
  }

  // ══════════════════════════════════════
  // 모듈 1: 브라우저 푸시 알림
  // ══════════════════════════════════════
  function setupNotifyButton() {
    var btn = document.getElementById('btn-notify');
    btn.addEventListener('click', function () {
      if (!notifyEnabled) {
        if (!('Notification' in window)) {
          alert('이 브라우저는 알림을 지원하지 않습니다.');
          return;
        }
        Notification.requestPermission().then(function (perm) {
          if (perm === 'granted') {
            notifyEnabled = true;
            btn.classList.add('active');
            btn.classList.remove('muted');
          }
        });
      } else {
        notifyEnabled = false;
        btn.classList.remove('active');
        btn.classList.add('muted');
      }
    });
  }

  function sendNotification(title, body) {
    if (!notifyEnabled || document.hasFocus()) return;
    try {
      var n = new Notification(title, { body: body, icon: '🏢', silent: true });
      setTimeout(function () { n.close(); }, 5000);
    } catch (e) { /* 무시 */ }
  }

  // ══════════════════════════════════════
  // 모듈 2: 알림 사운드 (Web Audio API)
  // ══════════════════════════════════════
  function setupSoundButton() {
    var btn = document.getElementById('btn-sound');
    btn.addEventListener('click', function () {
      soundEnabled = !soundEnabled;
      if (soundEnabled) {
        btn.classList.add('active');
        btn.classList.remove('muted');
        btn.textContent = '🔊';
      } else {
        btn.classList.remove('active');
        btn.classList.add('muted');
        btn.textContent = '🔇';
      }
    });
  }

  function getAudioCtx() {
    if (!audioCtx) {
      audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }
    return audioCtx;
  }

  function playSound(type) {
    if (!soundEnabled) return;
    try {
      var ctx = getAudioCtx();
      var osc = ctx.createOscillator();
      var gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      gain.gain.value = 0.15;

      var now = ctx.currentTime;

      if (type === 'enter') {
        // 밝은 상승 2음
        osc.type = 'sine';
        osc.frequency.setValueAtTime(523, now);
        osc.frequency.setValueAtTime(659, now + 0.1);
        gain.gain.setValueAtTime(0.15, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.25);
        osc.start(now);
        osc.stop(now + 0.25);
      } else if (type === 'profit') {
        // 상승 3음 짤랑
        osc.type = 'sine';
        osc.frequency.setValueAtTime(523, now);
        osc.frequency.setValueAtTime(659, now + 0.08);
        osc.frequency.setValueAtTime(784, now + 0.16);
        gain.gain.setValueAtTime(0.15, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);
        osc.start(now);
        osc.stop(now + 0.35);
      } else if (type === 'loss') {
        // 낮은 하강음
        osc.type = 'sawtooth';
        osc.frequency.setValueAtTime(330, now);
        osc.frequency.exponentialRampToValueAtTime(200, now + 0.3);
        gain.gain.setValueAtTime(0.1, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);
        osc.start(now);
        osc.stop(now + 0.35);
      } else if (type === 'alert') {
        // 빠른 반복음
        osc.type = 'square';
        osc.frequency.setValueAtTime(880, now);
        gain.gain.setValueAtTime(0.08, now);
        gain.gain.setValueAtTime(0, now + 0.05);
        gain.gain.setValueAtTime(0.08, now + 0.1);
        gain.gain.setValueAtTime(0, now + 0.15);
        gain.gain.setValueAtTime(0.08, now + 0.2);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.3);
        osc.start(now);
        osc.stop(now + 0.3);
      } else if (type === 'chat') {
        // 부드러운 톡
        osc.type = 'sine';
        osc.frequency.setValueAtTime(800, now);
        gain.gain.setValueAtTime(0.08, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.1);
        osc.start(now);
        osc.stop(now + 0.1);
      }
    } catch (e) { /* Web Audio 미지원 무시 */ }
  }

  // 이벤트 텍스트에서 사운드 타입 결정
  function detectSoundType(action) {
    if (!action) return null;
    if (action.includes('🚨') || action.includes('긴급')) return 'alert';
    if (action.includes('⚠️') || action.includes('에러')) return 'loss';
    if (action.includes('💰') || action.includes('청산')) {
      if (action.includes('-$') || action.includes('손실')) return 'loss';
      return 'profit';
    }
    if (action.includes('진입')) return 'enter';
    return null;
  }

  // ══════════════════════════════════════
  // 모듈 3: 팀 채팅
  // ══════════════════════════════════════
  function setupChatInput() {
    var input = document.getElementById('chat-input');
    var btn = document.getElementById('chat-send-btn');

    btn.addEventListener('click', sendChat);
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendChat();
      }
    });
  }

  function sendChat() {
    var input = document.getElementById('chat-input');
    var select = document.getElementById('chat-sender-select');
    var msg = input.value.trim();
    if (!msg) return;

    chatRef.push({
      who: select.value,
      message: msg,
      timestamp: firebase.database.ServerValue.TIMESTAMP
    });

    input.value = '';
  }

  function setupChatListeners() {
    chatRef.limitToLast(MAX_CHAT).on('child_added', function (snap) {
      var data = snap.val();
      if (!data) return;
      addChatMessage(data);
      playSound('chat');
    });
  }

  function addChatMessage(data) {
    var list = document.getElementById('chat-messages');
    var empty = document.getElementById('chat-empty');
    if (empty) empty.remove();

    var date = new Date(data.timestamp);
    var hours = String(date.getHours()).padStart(2, '0');
    var minutes = String(date.getMinutes()).padStart(2, '0');
    var timeStr = '[' + hours + ':' + minutes + ']';

    var char = characters.find(function (c) { return c.id === data.who; });
    var name = char ? char.name : data.who;
    var color = char ? char.color : '#aaa';

    var el = document.createElement('div');
    el.className = 'chat-msg';
    el.innerHTML =
      '<span class="chat-time">' + timeStr + '</span>' +
      '<span class="chat-sender" style="color: ' + color + '">' + escapeHtml(name) + '</span> ' +
      '<span class="chat-text">' + escapeHtml(data.message || '') + '</span>';

    list.appendChild(el);
    list.scrollTop = list.scrollHeight;

    chatCount++;
    if (chatCount > MAX_CHAT) {
      list.removeChild(list.firstChild);
      chatCount = MAX_CHAT;
    }
  }

  function escapeHtml(str) {
    var div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  // ══════════════════════════════════════
  // 모듈 4: 라이트/다크 모드
  // ══════════════════════════════════════
  function setupThemeButton() {
    var btn = document.getElementById('btn-theme');
    // localStorage에서 저장된 테마 복원
    var saved = localStorage.getItem('magi-theme');
    if (saved === 'light') {
      document.body.setAttribute('data-theme', 'light');
      btn.textContent = '☀️';
    }

    btn.addEventListener('click', function () {
      var current = document.body.getAttribute('data-theme');
      if (current === 'light') {
        document.body.removeAttribute('data-theme');
        btn.textContent = '🌙';
        localStorage.setItem('magi-theme', 'dark');
      } else {
        document.body.setAttribute('data-theme', 'light');
        btn.textContent = '☀️';
        localStorage.setItem('magi-theme', 'light');
      }
    });
  }

  // ══════════════════════════════════════
  // Firebase 연결 모니터링
  // ══════════════════════════════════════
  function setupConnectionMonitor(db) {
    var connRef = db.ref('.info/connected');
    var dot = document.getElementById('status-dot');
    var text = document.getElementById('status-text');

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

  // ══════════════════════════════════════
  // 사무실 렌더링
  // ══════════════════════════════════════
  function renderOffice() {
    var office = document.getElementById('office');

    characters.forEach(function (char) {
      var wrapper = document.createElement('div');
      wrapper.className = 'character-wrapper';
      wrapper.id = 'char-' + char.id;
      wrapper.style.left = (char.position.x - 32) + 'px';
      wrapper.style.top = (char.position.y - 32) + 'px';
      wrapper.style.setProperty('--char-color', char.color);
      wrapper.style.setProperty('--char-color-glow', char.color);

      var bubble = document.createElement('div');
      bubble.className = 'speech-bubble';
      bubble.id = 'bubble-' + char.id;
      wrapper.appendChild(bubble);

      var avatar = document.createElement('div');
      avatar.className = 'character-avatar status-idle';
      avatar.id = 'avatar-' + char.id;
      avatar.style.background = 'radial-gradient(circle at 35% 35%, ' +
        lightenColor(char.color, 30) + ', ' + char.color + ')';
      avatar.textContent = char.name[0];
      wrapper.appendChild(avatar);

      var name = document.createElement('div');
      name.className = 'character-name';
      name.textContent = char.name;
      wrapper.appendChild(name);

      var role = document.createElement('div');
      role.className = 'character-role';
      role.textContent = char.role;
      wrapper.appendChild(role);

      var desk = document.createElement('div');
      desk.className = 'desk' + (char.desk === 'large' ? ' large' : '');
      wrapper.appendChild(desk);

      wrapper.addEventListener('click', function () {
        openModal(char.id);
      });

      office.appendChild(wrapper);
    });
  }

  // ══════════════════════════════════════
  // 모바일 그리드
  // ══════════════════════════════════════
  function renderMobileGrid() {
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

  // ══════════════════════════════════════
  // Firebase 리스너
  // ══════════════════════════════════════
  function setupFirebaseListeners() {
    dbRef.on('value', function (snapshot) {
      var data = snapshot.val();
      if (!data) return;
      characters.forEach(function (char) {
        var state = data[char.id];
        if (state) updateCharacter(char.id, state);
      });
    });
  }

  // ══════════════════════════════════════
  // 이벤트 타임라인
  // ══════════════════════════════════════
  function setupEventListeners() {
    eventsRef.limitToLast(MAX_EVENTS).on('child_added', function (snap) {
      var event = snap.val();
      if (!event) return;
      addEventToTimeline(event);

      // 사운드 + 알림
      var soundType = detectSoundType(event.action);
      if (soundType) {
        playSound(soundType);
        var char = characters.find(function (c) { return c.id === event.who; });
        var name = char ? char.name : event.who;
        sendNotification('MAGI ' + name, event.action);
      }
    });
  }

  function addEventToTimeline(event) {
    var list = document.getElementById('timeline-list');
    var empty = document.getElementById('timeline-empty');
    if (empty) empty.remove();

    var date = new Date(event.timestamp);
    var hours = String(date.getHours()).padStart(2, '0');
    var minutes = String(date.getMinutes()).padStart(2, '0');
    var timeStr = '[' + hours + ':' + minutes + ']';

    var char = characters.find(function (c) { return c.id === event.who; });
    var name = char ? char.name : event.who;
    var color = char ? char.color : '#aaa';

    var el = document.createElement('div');
    el.className = 'timeline-event';
    el.innerHTML =
      '<span class="event-time">' + timeStr + '</span>' +
      '<span class="event-name" style="color: ' + color + '">' + name + '</span>' +
      '<span class="event-dash">—</span>' +
      '<span class="event-action">' + escapeHtml(event.action || '') + '</span>';

    list.insertBefore(el, list.firstChild);

    eventCount++;
    if (eventCount > MAX_EVENTS) {
      list.removeChild(list.lastChild);
      eventCount = MAX_EVENTS;
    }

    document.getElementById('timeline-count').textContent = eventCount + '건';
  }

  // ══════════════════════════════════════
  // 캐릭터 상태 업데이트
  // ══════════════════════════════════════
  function updateCharacter(id, state) {
    var now = Date.now();
    var status = state.status || 'offline';
    var mood = state.mood || 'idle';
    var task = state.task || '';
    var updatedAt = state.updatedAt || 0;

    if (updatedAt > 0 && (now - updatedAt) > AWAY_TIMEOUT_MS) {
      status = 'away';
      mood = 'idle';
    }

    var avatar = document.getElementById('avatar-' + id);
    if (avatar) {
      avatar.className = 'character-avatar';
      avatar.classList.add('status-' + status);
      if (mood !== 'idle') avatar.classList.add('mood-' + mood);
    }

    var bubble = document.getElementById('bubble-' + id);
    if (bubble) {
      bubble.className = 'speech-bubble';
      if (task && status !== 'offline' && !(status === 'idle' && mood === 'idle')) {
        bubble.textContent = task;
        bubble.title = task;
        bubble.classList.add('visible');
        if (mood === 'alert') bubble.classList.add('alert');
        if (status === 'away') bubble.classList.add('away');
      } else {
        bubble.textContent = '';
      }
    }

    var mobileDot = document.getElementById('mobile-dot-' + id);
    if (mobileDot) {
      var dotColors = { active: '#32CD32', idle: '#FFD700', away: '#888', offline: '#444' };
      mobileDot.style.background = dotColors[status] || '#444';
    }

    var mobileTask = document.getElementById('mobile-task-' + id);
    if (mobileTask) mobileTask.textContent = task || '대기 중';

    var modalOverlay = document.getElementById('modal-overlay');
    if (!modalOverlay.classList.contains('hidden') && modalOverlay.dataset.charId === id) {
      fillModal(id, state);
    }
  }

  // ══════════════════════════════════════
  // 모달
  // ══════════════════════════════════════
  function openModal(charId) {
    var char = characters.find(function (c) { return c.id === charId; });
    if (!char) return;

    var overlay = document.getElementById('modal-overlay');
    overlay.dataset.charId = charId;
    overlay.classList.remove('hidden');

    dbRef.child(charId).once('value', function (snap) {
      var state = snap.val() || { status: 'offline', mood: 'idle', task: '', updatedAt: 0 };
      fillModal(charId, state);
    });
  }

  function fillModal(charId, state) {
    var char = characters.find(function (c) { return c.id === charId; });
    if (!char) return;

    document.getElementById('modal-avatar').style.background =
      'radial-gradient(circle at 35% 35%, ' + lightenColor(char.color, 30) + ', ' + char.color + ')';

    document.getElementById('modal-name').textContent = char.name;
    document.getElementById('modal-role').textContent = char.role;

    var statusEl = document.getElementById('modal-status');
    var statusLabels = { active: '🟢 활동 중', idle: '🟡 대기', away: '⚪ 자리비움', offline: '⚫ 오프라인' };
    statusEl.textContent = statusLabels[state.status] || '⚫ 오프라인';

    document.getElementById('modal-task').textContent = state.task || '작업 없음';

    if (state.updatedAt) {
      document.getElementById('modal-updated').textContent =
        '마지막 업데이트: ' + new Date(state.updatedAt).toLocaleString('ko-KR');
    } else {
      document.getElementById('modal-updated').textContent = '';
    }
  }

  function closeModal() {
    document.getElementById('modal-overlay').classList.add('hidden');
  }

  // ══════════════════════════════════════
  // visibilitychange
  // ══════════════════════════════════════
  function setupVisibilityHandler() {
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) {
        if (dbRef) dbRef.off();
        if (eventsRef) eventsRef.off();
        if (chatRef) chatRef.off();
      } else {
        if (dbRef) setupFirebaseListeners();
        if (eventsRef) setupEventListeners();
        if (chatRef) setupChatListeners();
      }
    });
  }

  // ══════════════════════════════════════
  // 반응형
  // ══════════════════════════════════════
  function handleResize() {
    var wasMobile = isMobile;
    isMobile = window.innerWidth <= 767;
    if (isMobile && !wasMobile) renderMobileGrid();
    else if (!isMobile && wasMobile) {
      var grid = document.getElementById('mobile-grid');
      if (grid) grid.remove();
    }
  }

  // ══════════════════════════════════════
  // 유틸
  // ══════════════════════════════════════
  function showError(msg) {
    document.getElementById('error-message').textContent = msg;
    document.getElementById('error-banner').classList.remove('hidden');
  }

  function hideError() {
    document.getElementById('error-banner').classList.add('hidden');
  }

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

  // ══════════════════════════════════════
  // 오프라인 감지
  // ══════════════════════════════════════
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
