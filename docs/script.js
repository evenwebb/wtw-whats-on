    document.querySelectorAll('.cast-more-btn').forEach(function(btn) {
      btn.addEventListener('click', function() {
        var rest = btn.previousElementSibling;
        if (rest && rest.classList.contains('cast-rest')) {
          var on = rest.hasAttribute('hidden');
          if (on) { rest.removeAttribute('hidden'); btn.textContent = 'Less'; }
          else { rest.setAttribute('hidden', ''); btn.textContent = 'More'; }
        }
      });
    });
    (function() {
      var filmsMain = document.getElementById('films');
      var initialShowings = parseInt((filmsMain && filmsMain.getAttribute('data-initial-showings')) || '10', 10);
      var se = 'script';

      function showtimeFromRow(row) {
        if (!row) return { date: '', time: '', screen: '', cinema_name: '', booking_url: '', tags: [] };
        if (!Array.isArray(row)) return row;
        return {
          date: row[0] || '',
          time: row[1] || '',
          screen: row[2],
          cinema_name: row[3] || '',
          booking_url: row[4] || '',
          tags: Array.isArray(row[5]) ? row[5] : []
        };
      }
      function showtimesFromParsed(raw) {
        if (!raw) return [];
        if (raw.v === 1 && Array.isArray(raw.r)) return raw.r.map(showtimeFromRow);
        if (Array.isArray(raw)) {
          if (raw.length && Array.isArray(raw[0])) return raw.map(showtimeFromRow);
          return raw.slice();
        }
        return [];
      }
      function showtimeToCompactRow(st) {
        return [st.date || '', st.time || '', st.screen, st.cinema_name || '', st.booking_url || '', st.tags || []];
      }

      function escHtml(value) {
        return String(value || '')
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;');
      }
      function safeUrl(value) {
        var s = String(value || '').trim();
        return /^(https?:\/\/)/i.test(s) ? s : '#';
      }
      function formatDateLabel(isoDate) {
        if (!isoDate) return '';
        var dt = new Date(isoDate + 'T12:00:00Z');
        if (Number.isNaN(dt.getTime())) return isoDate;
        return dt.toLocaleDateString('en-GB', { weekday: 'short', day: '2-digit', month: 'short', timeZone: 'Europe/London' });
      }
      function tagHtml(tag) {
        var iconMap = {
          'Audio Description': 'icon-audio-desc',
          'Wheelchair access': 'icon-wheelchair',
          '2D': 'icon-2d',
          '3D': 'icon-3d',
          'Subtitles': 'icon-subtitles',
          'Silver Screen': 'icon-silver-screen',
          'Event cinema': 'icon-event-cinema',
          'Strobe Light warning': 'icon-strobe',
          'Parent & Baby': 'icon-parent-baby',
          'Autism Friendly': 'icon-autism-friendly',
          'Kids Club': 'icon-kids-club'
        };
        var shortLabelMap = { 'Audio Description': 'AD', 'Subtitles': 'Subs', 'Wheelchair access': 'WA', 'Strobe Light warning': 'Strobe' };
        var tooltipMap = {
          'Audio Description': 'Audio description',
          'Subtitles': 'Subtitled screening',
          'Wheelchair access': 'Wheelchair accessible',
          '2D': 'Standard 2D screening',
          'Strobe Light warning': 'Strobe lighting may affect photosensitive viewers'
        };
        var iconId = iconMap[tag];
        var label = shortLabelMap[tag] || tag;
        var tooltip = tooltipMap[tag] || (shortLabelMap[tag] ? tag : '');
        var titleAttr = tooltip ? ' title="' + escHtml(tooltip) + '"' : '';
        if (iconId) {
          return '<span class="tag"' + titleAttr + '><svg class="tag-icon" aria-hidden="true"><use href="#' + iconId + '"/></svg>' + escHtml(label) + '</span>';
        }
        return '<span class="tag"' + titleAttr + '>' + escHtml(label) + '</span>';
      }
      function renderShowingsHtml(list) {
        var grouped = {};
        list.forEach(function(st) {
          var d = st.date || '';
          if (!grouped[d]) grouped[d] = [];
          grouped[d].push(st);
        });
        return Object.keys(grouped).sort().map(function(d) {
          var rows = grouped[d].map(function(st) {
            var time = escHtml(st.time || '');
            var cinema = String(st.cinema_name || '').trim();
            cinema = cinema.indexOf(',') !== -1 ? cinema.split(',').pop().trim() : cinema;
            var screen = String(st.screen || '');
            var screenLabel = cinema && screen ? (cinema + ' (Screen ' + screen + ')') : (cinema || ('Screen ' + screen));
            var booking = String(st.booking_url || '');
            var timeEl = booking ? '<a href="' + safeUrl(booking) + '">' + time + '</a>' : '<span class="past">' + time + '</span>';
            var tags = Array.isArray(st.tags) ? st.tags.slice(0, 4) : [];
            var tagSpan = tags.map(tagHtml).join(' ');
            return '<div class="st-row"><span class="st-time">' + timeEl + '</span><span class="st-screen">' + escHtml(screenLabel) + '</span><span class="st-tags">' + tagSpan + '</span></div>';
          }).join('');
          return '<div class="day-group"><div class="st-date">' + escHtml(formatDateLabel(d)) + '</div>' + rows + '</div>';
        }).join('');
      }

      function sortShowtimes(list) {
        list.sort(function(a, b) {
          var ad = a.date || '';
          var bd = b.date || '';
          if (ad !== bd) return ad < bd ? -1 : ad > bd ? 1 : 0;
          var at = a.time || '';
          var bt = b.time || '';
          if (at !== bt) return at < bt ? -1 : at > bt ? 1 : 0;
          var as = String(a.screen || '');
          var bs = String(b.screen || '');
          if (as !== bs) return as < bs ? -1 : as > bs ? 1 : 0;
          var ab = a.booking_url || '';
          var bb = b.booking_url || '';
          return ab < bb ? -1 : ab > bb ? 1 : 0;
        });
      }

      function buildShowtimesInner(list) {
        var copy = list.slice();
        sortShowtimes(copy);
        var visible = copy.slice(0, initialShowings);
        var hidden = copy.slice(initialShowings);
        var html = renderShowingsHtml(visible);
        if (hidden.length) {
          var hiddenJson = JSON.stringify({ v: 1, r: hidden.map(showtimeToCompactRow) }).replace(/<\//g, '<\/');
          var count = hidden.length;
          var noun = count === 1 ? 'showing' : 'showings';
          html += '<' + se + ' type="application/json" class="showtimes-more-data">' + hiddenJson + '</' + se + '>';
          html += '<div class="showtimes-more" hidden></div>';
          html += '<button type="button" class="showtimes-more-btn" aria-expanded="false">Show ' + count + ' more ' + noun + '</button>';
        }
        return html;
      }

      function shortCinemaName(full) {
        var s = String(full || '').trim();
        if (!s) return '';
        var parts = s.split(',');
        return parts.length > 1 ? parts[parts.length - 1].trim() : s;
      }

      var selectedDate = 'all';
      var selectedCinema = 'all';
      var STORAGE_CINEMA = 'wtw-whats-on-cinema';

      function cinemaTabValues() {
        var vals = [];
        document.querySelectorAll('.tab-cinema').forEach(function(b) {
          var v = b.getAttribute('data-cinema');
          if (v != null && vals.indexOf(v) === -1) vals.push(v);
        });
        return vals;
      }

      function readPersistedCinema() {
        try {
          var raw = localStorage.getItem(STORAGE_CINEMA);
          if (raw == null || raw === '') return null;
          if (cinemaTabValues().indexOf(raw) !== -1) return raw;
        } catch (e0) {}
        try { localStorage.removeItem(STORAGE_CINEMA); } catch (e1) {}
        return null;
      }

      function writePersistedCinema(v) {
        try {
          if (v === 'all') localStorage.removeItem(STORAGE_CINEMA);
          else localStorage.setItem(STORAGE_CINEMA, v);
        } catch (e2) {}
      }

      function applyFilters() {
        var sectionVisibility = { now: false, coming: false };
        document.querySelectorAll('.film-card').forEach(function(card) {
          var dataScript = card.querySelector('script.film-showtimes-full');
          var showtimesEl = card.querySelector('.showtimes');
          if (!dataScript || !showtimesEl) {
            card.style.display = 'none';
            return;
          }
          var all = [];
          try {
            all = showtimesFromParsed(JSON.parse(dataScript.textContent || 'null'));
          } catch (e1) {
            card.style.display = 'none';
            return;
          }
          var picked = all.filter(function(st) {
            var dateOk = selectedDate === 'all' || st.date === selectedDate;
            var cinemaOk = selectedCinema === 'all' || shortCinemaName(st.cinema_name) === selectedCinema;
            return dateOk && cinemaOk;
          });
          var show = picked.length > 0;
          card.style.display = show ? 'block' : 'none';
          if (show) {
            var status = card.getAttribute('data-status') || '';
            if (status === 'now') sectionVisibility.now = true;
            if (status === 'coming-soon') sectionVisibility.coming = true;
            showtimesEl.innerHTML = buildShowtimesInner(picked);
          }
        });
        document.querySelectorAll('.film-section').forEach(function(section) {
          var sectionType = section.getAttribute('data-section') || '';
          var showSection = sectionType === 'now' ? sectionVisibility.now : sectionVisibility.coming;
          section.style.display = showSection ? 'grid' : 'none';
        });
      }

      document.querySelectorAll('.tab-date').forEach(function(btn) {
        btn.addEventListener('click', function() {
          document.querySelectorAll('.tab-date').forEach(function(b) { b.classList.remove('active'); });
          btn.classList.add('active');
          selectedDate = btn.getAttribute('data-date') || 'all';
          applyFilters();
        });
      });
      document.querySelectorAll('.tab-cinema').forEach(function(btn) {
        btn.addEventListener('click', function() {
          document.querySelectorAll('.tab-cinema').forEach(function(b) { b.classList.remove('active'); });
          btn.classList.add('active');
          selectedCinema = btn.getAttribute('data-cinema') || 'all';
          writePersistedCinema(selectedCinema);
          applyFilters();
        });
      });

      (function restoreCinemaFromStorage() {
        var saved = readPersistedCinema();
        if (saved == null) return;
        selectedCinema = saved;
        document.querySelectorAll('.tab-cinema').forEach(function(b) {
          if ((b.getAttribute('data-cinema') || '') === saved) b.classList.add('active');
          else b.classList.remove('active');
        });
        applyFilters();
      })();

      if (filmsMain) {
        filmsMain.addEventListener('click', function(e) {
          var btn = e.target.closest('.showtimes-more-btn');
          if (!btn || !filmsMain.contains(btn)) return;
          var card = btn.closest('.film-card');
          if (!card) return;
          var holder = card.querySelector('.showtimes-more');
          var dataNode = card.querySelector('.showtimes-more-data');
          if (!holder || !dataNode) return;

          var expanded = btn.getAttribute('aria-expanded') === 'true';
          if (expanded) {
            holder.setAttribute('hidden', '');
            btn.setAttribute('aria-expanded', 'false');
            btn.textContent = btn.getAttribute('data-show-label') || 'Show more';
            return;
          }

          if (!holder.hasChildNodes()) {
            try {
              var list = showtimesFromParsed(JSON.parse(dataNode.textContent || 'null'));
              holder.innerHTML = renderShowingsHtml(list);
            } catch (e2) {
              holder.innerHTML = '';
            }
          }
          holder.removeAttribute('hidden');
          btn.setAttribute('aria-expanded', 'true');
          if (!btn.getAttribute('data-show-label')) btn.setAttribute('data-show-label', btn.textContent);
          btn.textContent = 'Show less';
        });
      }
    })();
    (function() {
      var lb = document.getElementById('trailer-lightbox');
      var iframe = document.getElementById('trailer-lightbox-iframe');
      var backdrop = document.getElementById('trailer-lightbox-backdrop');
      var closeBtn = document.getElementById('trailer-lightbox-close');
      var fallbackLink = document.getElementById('trailer-lightbox-fallback');
      function closeLightbox() {
        lb.classList.remove('is-open');
        lb.setAttribute('aria-hidden', 'true');
        iframe.src = '';
        if (fallbackLink) fallbackLink.href = '#';
      }
      function openLightbox(embedUrl, watchUrl) {
        iframe.src = embedUrl;
        if (fallbackLink && watchUrl) fallbackLink.href = watchUrl;
        lb.classList.add('is-open');
        lb.setAttribute('aria-hidden', 'false');
      }
      document.querySelectorAll('.trailer-lightbox-trigger').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var embedUrl = this.getAttribute('data-embed');
          var watchUrl = this.getAttribute('data-watch') || '';
          if (embedUrl) openLightbox(embedUrl, watchUrl);
        });
      });
      if (backdrop) backdrop.addEventListener('click', closeLightbox);
      if (closeBtn) closeBtn.addEventListener('click', closeLightbox);
      document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && lb && lb.classList.contains('is-open')) closeLightbox();
      });
    })();
    function switchView(view) {
      document.querySelectorAll('.view-btn').forEach(function(b) { b.classList.toggle('active', b.dataset.view === view); });
      document.querySelector('.page').classList.toggle('poster-view', view === 'posters');
    }
