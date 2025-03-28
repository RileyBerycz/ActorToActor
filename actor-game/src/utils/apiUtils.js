// TMDB API constants
const BASE_URL = "https://api.themoviedb.org/3";
const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
const PROFILE_SIZE = "w185";
const POSTER_SIZE = "w342";

// Function to load actor data with appropriate region filtering
export const loadActorData = async (region = 'GLOBAL') => {
  const isDev = process.env.NODE_ENV === 'development';
  
  try {
    const dataUrl = isDev 
      ? `/actors_data_${region}.json` // Local path for development
      : `https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actors_data_${region}.json`;
    
    const response = await fetch(dataUrl);
    
    if (!response.ok) {
      throw new Error(`Failed to load actor data: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error('Error loading actor data:', error);
    throw error;
  }
};

// Get actor profile image URL
export const getActorImageUrl = (profilePath) => {
  if (!profilePath) return null;
  return `${BASE_IMG_URL}${PROFILE_SIZE}/${profilePath}`;
};

// Get media poster URL
export const getPosterUrl = (posterPath) => {
  if (!posterPath) return null;
  return `${BASE_IMG_URL}${POSTER_SIZE}/${posterPath}`;
};

// Function to fetch additional actor details if needed
export const fetchActorDetails = async (actorId, apiKey) => {
  try {
    const url = `${BASE_URL}/person/${actorId}?api_key=${apiKey}`;
    const response = await fetch(url);
    
    if (!response.ok) {
      throw new Error(`Failed to fetch actor details: ${response.status}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error(`Error fetching details for actor ${actorId}:`, error);
    throw error;
  }
};

// Fetches the shared works between two actors
export const fetchSharedWorks = async (actor1Id, actor2Id, actorData) => {
  const actor1 = actorData[actor1Id];
  const actor2 = actorData[actor2Id];
  
  if (!actor1 || !actor2) return [];
  
  // Get movie IDs for each actor
  const actor1MovieIds = (actor1.movie_credits || []).map(m => m.id);
  const actor2MovieIds = (actor2.movie_credits || []).map(m => m.id);
  
  // Get TV show IDs for each actor
  const actor1TvIds = (actor1.tv_credits || []).map(t => t.id);
  const actor2TvIds = (actor2.tv_credits || []).map(t => t.id);
  
  // Find common movie IDs
  const commonMovieIds = actor1MovieIds.filter(id => actor2MovieIds.includes(id));
  
  // Find common TV show IDs
  const commonTvIds = actor1TvIds.filter(id => actor2TvIds.includes(id));
  
  // Convert IDs to full media objects
  const commonMovies = commonMovieIds.map(id => {
    const movie1 = actor1.movie_credits.find(m => m.id === id);
    const movie2 = actor2.movie_credits.find(m => m.id === id);
    
    return {
      id,
      title: movie1?.title || movie2?.title || 'Unknown Movie',
      type: 'movie',
      poster_path: movie1?.poster_path || movie2?.poster_path,
      release_date: movie1?.release_date || movie2?.release_date
    };
  });
  
  const commonTvShows = commonTvIds.map(id => {
    const tv1 = actor1.tv_credits.find(t => t.id === id);
    const tv2 = actor2.tv_credits.find(t => t.id === id);
    
    return {
      id,
      name: tv1?.name || tv2?.name || 'Unknown TV Show',
      type: 'tv',
      poster_path: tv1?.poster_path || tv2?.poster_path,
      first_air_date: tv1?.first_air_date || tv2?.first_air_date
    };
  });
  
  // Return combined results, sorted by popularity if available
  return [...commonMovies, ...commonTvShows].sort((a, b) => 
    (b.popularity || 0) - (a.popularity || 0)
  );
};

export default {
  loadActorData,
  getActorImageUrl,
  getPosterUrl,
  fetchActorDetails,
  fetchSharedWorks
};