// 🏢 MAGI Pixel Office — sprite-generator.js
// Canvas API로 도트 캐릭터 + 사무실 배경 프로그래밍 생성
// 외부 이미지 파일 불필요!

var SpriteGenerator = (function () {
  'use strict';

  // 캐릭터별 도트 디자인 데이터
  var CHAR_DESIGNS = {
    junsu: { hair: '#FFD700', skin: '#FFD4A8', shirt: '#2a2a50', acc: 'headset', accColor: '#888' },
    byeol: { hair: '#7B68EE', skin: '#FFD4A8', shirt: '#3a2a60', acc: 'glasses', accColor: '#AAA' },
    mitsuri: { hair: '#FF69B4', hairTip: '#77DD77', skin: '#FFD4A8', shirt: '#4a2040', acc: 'hairpin', accColor: '#FF69B4' },
    sony: { hair: '#4169E1', skin: '#FFD4A8', shirt: '#1a3060', acc: 'pen', accColor: '#FFF' },
    asuka: { hair: '#FF6347', skin: '#FFD4A8', shirt: '#502020', acc: 'none', accColor: '' },
    kai: { hair: '#32CD32', skin: '#FFD4A8', shirt: '#1a4020', acc: 'none', accColor: '' },
    jet: { hair: '#FF8C00', skin: '#FFD4A8', shirt: '#4a3010', acc: 'none', accColor: '' },
    boomerang: { hair: '#00CED1', skin: '#FFD4A8', shirt: '#104040', acc: 'none', accColor: '' },
    release: { hair: '#9370DB', skin: '#FFD4A8', shirt: '#302050', acc: 'none', accColor: '' },
    shinji: { hair: '#87CEEB', skin: '#FFD4A8', shirt: '#203040', acc: 'none', accColor: '' },
    rei: { hair: '#E0E0E0', skin: '#FFD4A8', shirt: '#303030', acc: 'none', accColor: '' }
  };

  function hex2rgb(hex) {
    hex = hex.replace('#', '');
    return {
      r: parseInt(hex.substring(0, 2), 16),
      g: parseInt(hex.substring(2, 4), 16),
      b: parseInt(hex.substring(4, 6), 16)
    };
  }

  function darken(hex, pct) {
    var c = hex2rgb(hex);
    var f = 1 - pct / 100;
    return 'rgb(' + Math.floor(c.r * f) + ',' + Math.floor(c.g * f) + ',' + Math.floor(c.b * f) + ')';
  }

  function lighten(hex, pct) {
    var c = hex2rgb(hex);
    var f = pct / 100;
    return 'rgb(' + Math.min(255, Math.floor(c.r + (255 - c.r) * f)) + ',' +
      Math.min(255, Math.floor(c.g + (255 - c.g) * f)) + ',' +
      Math.min(255, Math.floor(c.b + (255 - c.b) * f)) + ')';
  }

  // 픽셀 그리기 헬퍼 (2x2 실제 픽셀 = 1 도트)
  var PX = 2; // 32x32 도트 → 64x64 실제

  function dot(ctx, x, y, color) {
    ctx.fillStyle = color;
    ctx.fillRect(x * PX, y * PX, PX, PX);
  }

  function dots(ctx, coords, color) {
    ctx.fillStyle = color;
    for (var i = 0; i < coords.length; i++) {
      ctx.fillRect(coords[i][0] * PX, coords[i][1] * PX, PX, PX);
    }
  }

  function hLine(ctx, x1, x2, y, color) {
    ctx.fillStyle = color;
    for (var x = x1; x <= x2; x++) {
      ctx.fillRect(x * PX, y * PX, PX, PX);
    }
  }

  function rect(ctx, x, y, w, h, color) {
    ctx.fillStyle = color;
    ctx.fillRect(x * PX, y * PX, w * PX, h * PX);
  }

  // ===== 캐릭터 생성 (64x64) =====
  function generateCharacter(charId) {
    var canvas = document.createElement('canvas');
    canvas.width = 64;
    canvas.height = 64;
    var ctx = canvas.getContext('2d');
    var d = CHAR_DESIGNS[charId];
    if (!d) return null;

    // 의자 (아래)
    rect(ctx, 10, 26, 12, 4, '#3a3a5a');
    rect(ctx, 11, 27, 10, 2, '#4a4a6a');

    // 책상 (아래)
    rect(ctx, 2, 24, 28, 3, '#3a3a5a');
    rect(ctx, 3, 23, 26, 1, '#4a4a6a');

    // 모니터 (책상 위)
    rect(ctx, 5, 18, 8, 5, '#222');
    rect(ctx, 6, 19, 6, 3, '#3366aa');
    // 모니터 받침
    rect(ctx, 8, 23, 2, 1, '#333');

    // 몸통
    rect(ctx, 12, 17, 8, 7, d.shirt);
    // 어깨
    rect(ctx, 10, 17, 2, 3, d.shirt);
    rect(ctx, 20, 17, 2, 3, d.shirt);

    // 팔 (키보드 위로)
    rect(ctx, 10, 20, 2, 3, d.skin);
    rect(ctx, 20, 20, 2, 3, d.skin);

    // 머리
    rect(ctx, 12, 8, 8, 9, d.skin);
    // 머리카락 (위)
    rect(ctx, 11, 6, 10, 3, d.hair);
    rect(ctx, 12, 9, 8, 2, d.hair);
    // 양옆 머리카락
    dots(ctx, [[11, 9], [11, 10], [20, 9], [20, 10]], d.hair);

    // 미츠리 투톤 머리 (핑크+초록)
    if (d.hairTip) {
      dots(ctx, [[11, 11], [12, 11], [20, 11], [19, 11]], d.hairTip);
      hLine(ctx, 11, 20, 7, d.hairTip);
    }

    // 눈
    dots(ctx, [[14, 12], [18, 12]], '#222');

    // 입
    dot(ctx, 16, 14, '#e88');

    // 악세서리
    if (d.acc === 'headset') {
      hLine(ctx, 10, 21, 5, d.accColor);
      dots(ctx, [[10, 11], [10, 12], [21, 11], [21, 12]], d.accColor);
    } else if (d.acc === 'glasses') {
      rect(ctx, 13, 11, 3, 2, d.accColor);
      rect(ctx, 17, 11, 3, 2, d.accColor);
      dot(ctx, 16, 11, d.accColor);
    } else if (d.acc === 'hairpin') {
      dots(ctx, [[20, 8], [21, 8], [21, 7]], d.accColor);
    } else if (d.acc === 'pen') {
      dots(ctx, [[22, 19], [22, 20], [22, 21], [22, 22]], d.accColor);
    }

    // 키보드 (책상 위)
    rect(ctx, 18, 22, 7, 1, '#555');
    rect(ctx, 19, 22, 5, 1, '#777');

    return canvas.toDataURL();
  }

  // ===== 사무실 배경 생성 (1280x720) =====
  function generateBackground() {
    var canvas = document.createElement('canvas');
    canvas.width = 1280;
    canvas.height = 720;
    var ctx = canvas.getContext('2d');

    // 벽 (상단)
    ctx.fillStyle = '#16162e';
    ctx.fillRect(0, 0, 1280, 200);

    // 벽과 바닥 경계선
    ctx.fillStyle = '#2a2a4a';
    ctx.fillRect(0, 198, 1280, 4);

    // 바닥 — 체크무늬 타일
    var tileSize = 40;
    for (var ty = 200; ty < 720; ty += tileSize) {
      for (var tx = 0; tx < 1280; tx += tileSize) {
        var isLight = ((tx / tileSize) + (ty / tileSize)) % 2 === 0;
        ctx.fillStyle = isLight ? '#1e1e36' : '#1a1a30';
        ctx.fillRect(tx, ty, tileSize, tileSize);
      }
    }

    // 벽에 MAGI 로고 (도트 텍스트)
    ctx.fillStyle = '#FFD700';
    ctx.font = 'bold 28px monospace';
    ctx.textAlign = 'center';
    ctx.fillText('⚡ M A G I', 640, 60);

    // 벽 장식 — 라인 차트 (화이트보드 느낌)
    ctx.strokeStyle = '#3a3a5a';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.rect(550, 80, 180, 100);
    ctx.stroke();

    ctx.strokeStyle = '#32CD32';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(560, 160);
    ctx.lineTo(580, 140);
    ctx.lineTo(600, 150);
    ctx.lineTo(620, 120);
    ctx.lineTo(640, 130);
    ctx.lineTo(660, 100);
    ctx.lineTo(680, 110);
    ctx.lineTo(700, 95);
    ctx.lineTo(720, 105);
    ctx.stroke();

    // 서버랙 (왼쪽 벽)
    ctx.fillStyle = '#222240';
    ctx.fillRect(80, 40, 60, 150);
    ctx.fillStyle = '#333355';
    ctx.fillRect(85, 45, 50, 20);
    ctx.fillRect(85, 70, 50, 20);
    ctx.fillRect(85, 95, 50, 20);
    ctx.fillRect(85, 120, 50, 20);
    // LED
    ctx.fillStyle = '#32CD32';
    ctx.fillRect(90, 50, 4, 4);
    ctx.fillRect(90, 75, 4, 4);
    ctx.fillStyle = '#FF8C00';
    ctx.fillRect(90, 100, 4, 4);
    ctx.fillStyle = '#32CD32';
    ctx.fillRect(90, 125, 4, 4);

    // 커피머신 (오른쪽 아래)
    ctx.fillStyle = '#333';
    ctx.fillRect(1100, 580, 40, 50);
    ctx.fillStyle = '#555';
    ctx.fillRect(1105, 585, 30, 15);
    // 컵
    ctx.fillStyle = '#FFF';
    ctx.fillRect(1150, 610, 12, 16);
    ctx.fillStyle = '#8B4513';
    ctx.fillRect(1152, 612, 8, 8);

    // 화분 (왼쪽 아래)
    ctx.fillStyle = '#8B4513';
    ctx.fillRect(60, 620, 30, 25);
    ctx.fillStyle = '#228B22';
    ctx.fillRect(62, 600, 10, 22);
    ctx.fillRect(72, 605, 10, 17);
    ctx.fillRect(80, 608, 8, 14);

    return canvas.toDataURL();
  }

  return {
    generateCharacter: generateCharacter,
    generateBackground: generateBackground,
    CHAR_DESIGNS: CHAR_DESIGNS
  };
})();
