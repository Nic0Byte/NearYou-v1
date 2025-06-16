// Global state
let token = "";
let username = "";
let map, userMarker, userPopup, userPolyline;
let shopsMarkers = [];
let allShops = [];
let currentPosition = null;
let routePoints = [];
let notifications = [];
let startTime = null;
let categoryFilter = "all";
let isDarkTheme = false;
let userData = null;
let notificationsPage = 0;
let isLoadingMoreNotifications = false;
let lazyLoadObserver = null;

// WebSocket variables
let websocket = null;
let isReconnecting = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_INTERVAL = 3000; // 3 secondi

// User profile data
const DEFAULT_USER_PROFILE = {
  id: null,
  age: "--",
  profession: "--",
  interests: "--"
};

// DOM Elements
const loadingOverlay = document.getElementById("loading-overlay");
const containerEl = document.querySelector(".container");
const loginCard = document.getElementById("login-card");
const sidebarEl = document.getElementById("sidebar");
const welcomeText = document.getElementById("welcomeText");
const userAvatar = document.getElementById("user-avatar");

// Local cache for fetched data
const localCache = {
  shopAreas: {},  // Cache for shops in different areas
  notifications: [],  // Cache for loaded notifications
  profile: null  // Cache for user profile
};

// Initialize
document.addEventListener("DOMContentLoaded", function() {
  // UI event handlers
  document.getElementById("btn").onclick = handleLogin;
  document.getElementById("logoutButton").onclick = handleLogout;
  document.getElementById("theme-toggle").onclick = toggleTheme;
  document.getElementById("center-map").onclick = centerMapOnUser;
  document.getElementById("clear-route").onclick = clearRoute;
  document.querySelector(".sidebar-toggle").onclick = toggleSidebar;
  document.getElementById("load-more-notifications").onclick = loadMoreNotifications;
  
  // Set up category filters
  const categoryPills = document.querySelectorAll(".category-pill");
  categoryPills.forEach(pill => {
    pill.addEventListener("click", () => {
      categoryPills.forEach(p => p.classList.remove("active"));
      pill.classList.add("active");
      categoryFilter = pill.dataset.category;
      filterShopsByCategory();
    });
  });
  
  // Set up Intersection Observer for lazy loading
  setupLazyLoadingObservers();
  
  // Check for stored theme preference
  const storedTheme = localStorage.getItem("nearYouTheme");
  if (storedTheme === "dark") {
    toggleTheme();
  }
  
  // Check for existing token
  const storedToken = sessionStorage.getItem("nearYouToken");
  if (storedToken) {
    token = storedToken;
    username = sessionStorage.getItem("nearYouUsername") || "";
    processLogin();
  } else {
    setTimeout(() => {
      loadingOverlay.style.display = "none";
    }, 500);
  }
});

function setupLazyLoadingObservers() {
  // Observer for lazy loading notifications
  lazyLoadObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting && !isLoadingMoreNotifications) {
        console.log("Notification sentinel is visible, loading more notifications");
        loadMoreNotifications();
      }
    });
  }, {
    rootMargin: '0px 0px 100px 0px' // Load when 100px from viewport
  });
  
  // Observe the sentinel element
  const sentinel = document.querySelector('.notification-sentinel');
  if (sentinel) {
    lazyLoadObserver.observe(sentinel);
  }
  
  // Observer for stats section (load only when visible)
  const statsObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        console.log("Stats section is visible, fetching stats");
        fetchUserStats();
        statsObserver.unobserve(entry.target); // Only need to load once
      }
    });
  });
  
  // Observe stats section
  const statsSection = document.querySelector('.stats-grid');
  if (statsSection) {
    statsObserver.observe(statsSection);
  }
}

function handleLogin() {
  const usernameInput = document.getElementById("user").value.trim();
  const pwd = document.getElementById("pass").value;
  document.getElementById("err").textContent = "";
  
  if (!usernameInput || !pwd) {
    document.getElementById("err").textContent = "Compila entrambi i campi.";
    return;
  }
  
  loadingOverlay.style.display = "flex";
  
  fetch("/api/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ username: usernameInput, password: pwd }),
  })
  .then(res => res.json())
  .then(data => {
    if (!data.access_token) {
      throw new Error("Token non ricevuto");
    }
    token = data.access_token;
    username = usernameInput;
    
    // Store in session storage
    sessionStorage.setItem("nearYouToken", token);
    sessionStorage.setItem("nearYouUsername", username);
    
    processLogin();
  })
  .catch(err => {
    loadingOverlay.style.display = "none";
    document.getElementById("err").textContent = err.message || "Errore di login";
  });
}

function processLogin() {
  // Set welcome text and avatar
  welcomeText.textContent = `${username}`;
  userAvatar.textContent = username.charAt(0).toUpperCase();
  
  // Hide login, show container
  loginCard.style.display = "none";
  containerEl.style.display = "flex";
  
  // Initialize map and start update loop
  initMap();
  fetchUserProfile();
  
  // Set timer
  startTime = new Date();
  
  // Setup WebSocket connection for real-time updates
  setupWebSocket();
  
  // Fallback to polling if WebSocket setup fails after 5 seconds
  setTimeout(() => {
    if (!websocket || websocket.readyState !== WebSocket.OPEN) {
      fallbackToPolling();
    }
  }, 5000);
  
  // Update active time
  setInterval(updateActiveTime, 60000);
  
  // Load initial notifications
  loadMoreNotifications();
  
  // Hide loading overlay
  setTimeout(() => {
    loadingOverlay.style.display = "none";
  }, 500);
}

function handleLogout() {
  // Chiudi il WebSocket
  closeWebSocket();
  
  token = "";
  username = "";
  userData = { ...DEFAULT_USER_PROFILE };
  clearMap();
  notifications = [];
  notificationsPage = 0;
  
  // Clear storage
  sessionStorage.removeItem("nearYouToken");
  sessionStorage.removeItem("nearYouUsername");
  
  // Clear caches
  localCache.shopAreas = {};
  localCache.notifications = [];
  localCache.profile = null;
  
  // Refresh the page
  window.location.reload();
}

function toggleTheme() {
  isDarkTheme = !isDarkTheme;
  if (isDarkTheme) {
    document.body.classList.add("dark-theme");
    document.getElementById("theme-toggle").innerHTML = '<i class="fas fa-sun"></i>';
    localStorage.setItem("nearYouTheme", "dark");
  } else {
    document.body.classList.remove("dark-theme");
    document.getElementById("theme-toggle").innerHTML = '<i class="fas fa-moon"></i>';
    localStorage.setItem("nearYouTheme", "light");
  }
}

function toggleSidebar() {
  sidebarEl.classList.toggle("active");
}

function initMap() {
  map = L.map("map").setView([45.4642, 9.19], 15);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© OpenStreetMap contributors"
  }).addTo(map);
  
  const userIcon = L.icon({
    iconUrl: "https://maps.google.com/mapfiles/kml/shapes/cycling.png",
    iconSize: [32, 32],
    iconAnchor: [16, 32],
  });
  
  userMarker = L.marker([45.4642, 9.19], { icon: userIcon });
  userPolyline = L.polyline([], { color: 'blue', weight: 3 });
  
  // Force map size calculation
  setTimeout(() => {
    map.invalidateSize();
  }, 300);
  
  // Setup map event listeners for lazy loading
  setupMapListeners();
}

function setupMapListeners() {
  // Lazy load shops when map view changes
  map.on('moveend', function() {
    console.log("Map view changed, loading shops for visible area");
    fetchShopsInVisibleArea();
  });
  
  // Also load shops when map is first initialized
  fetchShopsInVisibleArea();
}

function clearMap() {
  if (map) {
    if (userMarker && userMarker._map) map.removeLayer(userMarker);
    if (userPolyline && userPolyline._map) map.removeLayer(userPolyline);
    
    // Clear shop markers
    shopsMarkers.forEach(marker => {
      if (marker._map) map.removeLayer(marker);
    });
    shopsMarkers = [];
    
    // Reset route
    routePoints = [];
  }
}

// WebSocket setup and management
function setupWebSocket() {
  if (!token || websocket) return;
  
  // Determina il protocollo corretto (ws o wss) in base all'URL corrente
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const host = window.location.host;
  const wsUrl = `${protocol}//${host}/ws/positions`;
  
  console.log(`Connecting to WebSocket at ${wsUrl}`);
  
  // Crea una nuova connessione WebSocket
  websocket = new WebSocket(wsUrl);
  
  // Gestione dell'apertura della connessione
  websocket.onopen = () => {
    console.log('WebSocket connection established');
    reconnectAttempts = 0;
    
    // Invia il token di autenticazione
    websocket.send(JSON.stringify({
      token: token
    }));
  };
  
  // Gestione dei messaggi in arrivo
  websocket.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      
      // Gestisci diversi tipi di messaggi
      if (data.type === 'connection_established') {
        console.log(`WebSocket connection confirmed for user ${data.user_id}`);
      }
      else if (data.type === 'position_update') {
        // Aggiorna la posizione dell'utente sulla mappa
        updateUserPosition(data.data);
      }
      else if (data.error) {
        console.error(`WebSocket error: ${data.error}`);
        closeWebSocket();
      }
    } catch (e) {
      console.error('Error parsing WebSocket message:', e);
    }
  };
  
  // Gestione degli errori
  websocket.onerror = (error) => {
    console.error('WebSocket error:', error);
  };
  
  // Gestione della chiusura della connessione
  websocket.onclose = (event) => {
    console.log(`WebSocket connection closed with code ${event.code}`);
    websocket = null;
    
    // Tenta di riconnettersi se non è stata una chiusura volontaria
    if (!isReconnecting && event.code !== 1000) {
      reconnectWebSocket();
    }
  };
}

// Funzione per riconnettere il WebSocket in caso di interruzione
function reconnectWebSocket() {
  if (isReconnecting || reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) return;
  
  isReconnecting = true;
  reconnectAttempts++;
  
  console.log(`Attempting to reconnect WebSocket (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
  
  // Mostra notifica di riconnessione
  showReconnectingNotification();
  
  // Attendi prima di ritentare
  setTimeout(() => {
    isReconnecting = false;
    setupWebSocket();
  }, RECONNECT_INTERVAL);
}

// Funzione per chiudere volontariamente la connessione WebSocket
function closeWebSocket() {
  if (websocket) {
    websocket.close(1000, "Disconnessione volontaria");
    websocket = null;
  }
}

// Mostra una notifica di riconnessione all'utente
function showReconnectingNotification() {
  // Aggiungi un elemento di notifica connessione nella parte superiore della mappa
  const reconnectDiv = document.createElement('div');
  reconnectDiv.className = 'reconnect-notification';
  reconnectDiv.innerHTML = `<i class="fas fa-sync-alt fa-spin"></i> Riconnessione in corso...`;
  
  // Aggiungi o sostituisci l'elemento esistente
  const existingNotification = document.querySelector('.reconnect-notification');
  if (existingNotification) {
    existingNotification.replaceWith(reconnectDiv);
  } else {
    document.getElementById('map-container').appendChild(reconnectDiv);
  }
  
  // Rimuovi dopo 3 secondi
  setTimeout(() => {
    reconnectDiv.remove();
  }, 3000);
}

// Fallback al polling HTTP
async function pollUserPosition() {
  if (!token) return;
  try {
    const res = await fetch("/api/user/positions", {
      headers: { Authorization: `Bearer ${token}` }
    });
    const { positions } = await res.json();
    
    // Update UI only if we got positions
    if (positions && positions.length > 0) {
      updateUserPosition(positions[0]);
    }
  } catch (err) {
    console.error("Error fetching position:", err);
  }
}

// Funzione per fare fallback al polling HTTP se WebSocket fallisce
function fallbackToPolling() {
  console.log("Fallback to HTTP polling due to WebSocket failure");
  
  // Se il WebSocket fallisce, usa il vecchio metodo di polling
  pollUserPosition();
  setInterval(pollUserPosition, 3000);
}

function updateUserPosition(positionData) {
  const { latitude, longitude, message, user_id } = positionData;
  const latlng = [latitude, longitude];
  
  // Store position and update route
  currentPosition = latlng;
  routePoints.push(latlng);
  
  // Update map
  if (!userMarker._map) {
    userMarker.setLatLng(latlng).addTo(map);
    map.setView(latlng, 15);
  } else {
    userMarker.setLatLng(latlng);
  }
  
  // Update polyline
  userPolyline.setLatLngs(routePoints);
  if (!userPolyline._map) {
    userPolyline.addTo(map);
  }
  
  // Update notification if we have a message
  if (message && message.trim() !== "") {
    // Check if this is a new message
    const isNewMessage = !notifications.some(n => n.message === message);
    
    if (isNewMessage) {
      // Get the current timestamp
      const timestamp = new Date().toLocaleTimeString();
      
      // Determine the closest shop
      let closestShop = null;
      if (allShops.length > 0) {
        closestShop = findClosestShop(latlng);
      }
      
      // Create notification
      const notification = {
        id: Date.now(),
        message: message,
        timestamp: timestamp,
        shopName: closestShop ? closestShop.name : "Negozio nelle vicinanze",
        category: closestShop ? closestShop.category : "shopping"
      };
      
      // Add to notifications and update UI
      notifications.unshift(notification);
      localCache.notifications.unshift(notification);
      updateNotifications();
      
      // Show popup on map
      if (userPopup) {
        map.closePopup(userPopup);
      }
      
      userPopup = L.popup({
        className: 'custom-popup'
      })
      .setLatLng(latlng)
      .setContent(`
        <div class="popup-header">${notification.shopName}</div>
        <div class="popup-content">
          <div>Sei vicino a questo negozio!</div>
          <div class="popup-message">${message}</div>
        </div>
      `)
      .openOn(map);
      
      // Update notification count
      document.getElementById("total-notifications").textContent = notifications.length;
    }
  }
  
  // Update total distance
  if (routePoints.length > 1) {
    const totalDistance = calculateRouteDistance(routePoints);
    document.getElementById("total-distance").textContent = totalDistance.toFixed(1);
  }
  
  // Check if we need to load shops for this area
  if (map && shouldLoadShopsForPosition(latlng)) {
    fetchShopsInVisibleArea();
  }
}

function shouldLoadShopsForPosition(position) {
  // Check if we have already loaded shops for this area
  const bounds = map.getBounds();
  if (!bounds.contains(position)) {
    return true;
  }
  
  // Also reload if we have very few shops
  if (allShops.length < 5) {
    return true;
  }
  
  return false;
}

function updateNotifications() {
  const container = document.getElementById("notifications-container");
  
  // Get the latest notifications
  const recentNotifications = localCache.notifications.slice(0, 10);
  
  // Clear container first
  container.innerHTML = "";
  
  if (recentNotifications.length === 0) {
    container.innerHTML = '<div class="notification-item">Nessuna notifica ricevuta</div>';
    return;
  }
  
  recentNotifications.forEach(notification => {
    // Choose icon based on category
    let iconClass = "fas fa-shopping-bag";
    if (notification.category === "ristorante" || notification.category === "bar") {
      iconClass = "fas fa-utensils";
    } else if (notification.category === "abbigliamento") {
      iconClass = "fas fa-tshirt";
    } else if (notification.category === "supermercato") {
      iconClass = "fas fa-shopping-cart";
    } else if (notification.category === "elettronica") {
      iconClass = "fas fa-laptop";
    }
    
    const notificationEl = document.createElement("div");
    notificationEl.className = "notification-item";
    notificationEl.innerHTML = `
      <div class="notification-icon">
        <i class="${iconClass}"></i>
      </div>
      <div class="notification-content">
        <div class="notification-title">${notification.shopName}</div>
        <div class="notification-desc">${notification.message}</div>
        <div class="notification-time">${notification.timestamp}</div>
      </div>
    `;
    container.appendChild(notificationEl);
  });
}

async function fetchUserProfile() {
  if (!token) return;
  
  // Check if we have cached profile data
  if (localCache.profile) {
    userData = { ...localCache.profile };
    updateProfileUI();
    return;
  }
  
  try {
    // Fetch profile data from the API
    const res = await fetch("/api/user/profile", {
      headers: { Authorization: `Bearer ${token}` }
    });
    
    let profileData;
    
    try {
      profileData = await res.json();
      
      // Cache the profile data
      localCache.profile = {
        id: profileData.user_id,
        age: profileData.age,
        profession: profileData.profession,
        interests: profileData.interests
      };
      
    } catch (e) {
      console.error("Error parsing profile data:", e);
      // Fallback profile data
      profileData = {
        user_id: 1,
        age: Math.floor(Math.random() * 30) + 20,
        profession: ["Ingegnere", "Avvocato", "Medico", "Insegnante", "Studente"][Math.floor(Math.random() * 5)],
        interests: ["Tecnologia, Sport, Cinema", "Viaggi, Cibo, Libri", "Musica, Arte, Fotografia"][Math.floor(Math.random() * 3)]
      };
    }
    
    userData = {
      id: profileData.user_id,
      age: profileData.age,
      profession: profileData.profession,
      interests: profileData.interests
    };
    
    updateProfileUI();
    
  } catch (err) {
    console.error("Error fetching user profile:", err);
  }
}

function updateProfileUI() {
  // Update UI elements with user data
  document.getElementById("user-id").textContent = `ID: ${userData.id}`;
  document.getElementById("user-age").textContent = `Età: ${userData.age}`;
  document.getElementById("user-job").textContent = `Professione: ${userData.profession}`;
  document.getElementById("user-interests").textContent = `Interessi: ${userData.interests}`;
}

async function fetchUserStats() {
  if (!token) return;
  
  try {
    const res = await fetch("/api/user/stats", {
      headers: { Authorization: `Bearer ${token}` }
    });
    
    const stats = await res.json();
    
    // Update stats UI
    document.getElementById("total-notifications").textContent = stats.notifications || "0";
    
    // The other stats are updated elsewhere:
    // - total-distance: updated in updateUserPosition
    // - shops-nearby: updated in fetchShopsInVisibleArea
    // - active-time: updated in updateActiveTime
    
  } catch (err) {
    console.error("Error fetching user stats:", err);
  }
}

async function fetchShopsInVisibleArea() {
  if (!token || !map) return;
  
  // Get current map bounds
  const bounds = map.getBounds();
  const visibleArea = {
    north: bounds.getNorth(),
    south: bounds.getSouth(),
    east: bounds.getEast(),
    west: bounds.getWest()
  };
  
  // Create cache key based on visible area (rounded to reduce variations)
  const cacheKey = `${visibleArea.west.toFixed(4)},${visibleArea.south.toFixed(4)},${visibleArea.east.toFixed(4)},${visibleArea.north.toFixed(4)}`;
  
  // Check if we have already loaded shops for this area
  if (localCache.shopAreas[cacheKey]) {
    console.log("Using cached shops for this area");
    allShops = localCache.shopAreas[cacheKey];
    filterShopsByCategory();
    return;
  }
  
  try {
    console.log("Fetching shops for visible area:", visibleArea);
    
    // Try to fetch shops from the API
    const res = await fetch(`/api/shops/inArea?n=${visibleArea.north}&s=${visibleArea.south}&e=${visibleArea.east}&w=${visibleArea.west}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    
    // Process API response
    let shops;
    try {
      shops = await res.json();
    } catch (e) {
      console.error("Error parsing shops data:", e);
      // If API fails, generate fallback data in visible area
      shops = generateFallbackShopsInArea(visibleArea);
    }
    
    // Normalize shop data format
    const normalizedShops = shops.map(shop => ({
      id: shop.id || shop.shop_id,
      name: shop.shop_name || shop.name,
      category: shop.category,
      lat: shop.lat || (shop.geom ? shop.geom.coordinates[1] : 0),
      lon: shop.lon || (shop.geom ? shop.geom.coordinates[0] : 0)
    }));
    
    // Update the global shops array
    allShops = normalizedShops;
    
    // Cache the results for this area
    localCache.shopAreas[cacheKey] = normalizedShops;
    
    // Update the map with the new shops
    filterShopsByCategory();
    
    // Update shop count
    document.getElementById("shops-nearby").textContent = allShops.length;
    
  } catch (err) {
    console.error("Error fetching shops:", err);
  }
}

function generateFallbackShopsInArea(area) {
  const categories = ["ristorante", "bar", "abbigliamento", "supermercato", "elettronica"];
  const shops = [];
  
  // Generate 10 random shops within the visible area
  for (let i = 1; i <= 10; i++) {
    const lat = area.south + (Math.random() * (area.north - area.south));
    const lon = area.west + (Math.random() * (area.east - area.west));
    
    shops.push({
      id: i,
      shop_name: `Negozio ${i}`,
      category: categories[Math.floor(Math.random() * categories.length)],
      lat: lat,
      lon: lon
    });
  }
  
  return shops;
}

function filterShopsByCategory() {
  // Filter shops by selected category
  let filteredShops = [...allShops];
  
  if (categoryFilter !== "all") {
    filteredShops = allShops.filter(shop => shop.category === categoryFilter);
  }
  
  // Update shop markers on the map
  updateShopMarkers(filteredShops);
  
  // Update count in UI
  document.getElementById("shops-nearby").textContent = filteredShops.length;
}

function updateShopMarkers(shops) {
  // Clear existing markers
  shopsMarkers.forEach(marker => {
    if (marker._map) map.removeLayer(marker);
  });
  shopsMarkers = [];
  
  // Add new markers
  shops.forEach(shop => {
    // Choose icon based on category
    let iconUrl = "https://maps.google.com/mapfiles/ms/icons/red.png";
    if (shop.category === "ristorante" || shop.category === "bar") {
      iconUrl = "https://maps.google.com/mapfiles/ms/icons/yellow.png";
    } else if (shop.category === "supermercato") {
      iconUrl = "https://maps.google.com/mapfiles/ms/icons/green.png";
    } else if (shop.category === "elettronica") {
      iconUrl = "https://maps.google.com/mapfiles/ms/icons/blue.png";
    }
    
    const shopIcon = L.icon({
      iconUrl: iconUrl,
      iconSize: [32, 32],
      iconAnchor: [16, 32],
      popupAnchor: [0, -32]
    });
    
    const marker = L.marker([shop.lat, shop.lon], { icon: shopIcon })
      .bindPopup(`
        <div class="custom-popup">
          <div class="popup-header">${shop.name}</div>
          <div class="popup-content">
            <div>Categoria: ${shop.category}</div>
            <div>ID: ${shop.id}</div>
          </div>
        </div>
      `);
    
    marker.addTo(map);
    shopsMarkers.push(marker);
  });
}

function findClosestShop(position) {
  if (!allShops || allShops.length === 0) return null;
  
  // Find closest shop based on distance
  let closestShop = null;
  let minDistance = Infinity;
  
  allShops.forEach(shop => {
    const distance = calculateHaversineDistance(
      position[0], position[1], 
      shop.lat, shop.lon
    );
    
    if (distance < minDistance) {
      minDistance = distance;
      closestShop = shop;
    }
  });
  
  return closestShop;
}

async function loadMoreNotifications() {
  if (!token || isLoadingMoreNotifications) return;
  
  isLoadingMoreNotifications = true;
  
  try {
    const loadMoreBtn = document.getElementById("load-more-notifications");
    loadMoreBtn.textContent = "Caricamento...";
    
    const res = await fetch(`/api/user/promotions?offset=${notificationsPage * 10}&limit=10`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    
    const data = await res.json();
    
    if (data.promotions && data.promotions.length > 0) {
      // Process and add to notifications
      const newNotifications = data.promotions.map(p => ({
        id: p.event_id,
        message: p.message,
        timestamp: new Date(p.timestamp).toLocaleTimeString(),
        shopName: p.shop_name,
        category: determineCategory(p.shop_name)
      }));
      
      // Append to local cache
      localCache.notifications = [...localCache.notifications, ...newNotifications];
      
      // Update UI
      updateNotifications();
      
      // Increment page counter
      notificationsPage++;
      
      // Show/hide load more button
      if (data.promotions.length < 10) {
        loadMoreBtn.style.display = "none";
      } else {
        loadMoreBtn.textContent = "Carica altre notifiche";
      }
    } else {
      loadMoreBtn.style.display = "none";
    }
  } catch (err) {
    console.error("Error loading more notifications:", err);
  } finally {
    isLoadingMoreNotifications = false;
  }
}

function determineCategory(shopName) {
  // Simple heuristic to determine shop category from name
  shopName = shopName.toLowerCase();
  
  if (shopName.includes("ristorante") || shopName.includes("trattoria") || shopName.includes("pizzeria")) {
    return "ristorante";
  } else if (shopName.includes("bar") || shopName.includes("caffè") || shopName.includes("cafe")) {
    return "bar";
  } else if (shopName.includes("super") || shopName.includes("market")) {
    return "supermercato";
  } else if (shopName.includes("tech") || shopName.includes("elettro")) {
    return "elettronica";
  } else if (shopName.includes("moda") || shopName.includes("abbiglia")) {
    return "abbigliamento";
  }
  
  return "shopping";
}

function centerMapOnUser() {
  if (currentPosition) {
    map.setView(currentPosition, 15);
  }
}

function clearRoute() {
  routePoints = [];
  if (currentPosition) {
    routePoints.push(currentPosition);
  }
  userPolyline.setLatLngs(routePoints);
  document.getElementById("total-distance").textContent = "0";
}

function updateActiveTime() {
  if (!startTime) return;
  
  const now = new Date();
  const diffMinutes = Math.floor((now - startTime) / 60000);
  document.getElementById("active-time").textContent = diffMinutes;
}

// Utility Functions
function calculateHaversineDistance(lat1, lon1, lat2, lon2) {
  const R = 6371; // Earth radius in km
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = 
    Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * 
    Math.sin(dLon/2) * Math.sin(dLon/2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  const d = R * c;
  return d;
}

function toRad(deg) {
  return deg * Math.PI / 180;
}

function calculateRouteDistance(points) {
  let distance = 0;
  for (let i = 1; i < points.length; i++) {
    distance += calculateHaversineDistance(
      points[i-1][0], points[i-1][1],
      points[i][0], points[i][1]
    );
  }
  return distance;
}