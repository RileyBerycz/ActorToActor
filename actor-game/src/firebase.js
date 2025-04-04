import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyCZnu8swxNF-csr9iMFxWTgsbMcQ3QW0nI",
  authDomain: "actortoactor-c163f.firebaseapp.com",
  projectId: "actortoactor-c163f",
  storageBucket: "actortoactor-c163f.firebasestorage.app",
  messagingSenderId: "1085181706472",
  appId: "1:1085181706472:web:616e5758de1771f5d9095d"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

export { db };