"use client";

import dataProviderSimpleRest from "@refinedev/simple-rest";

// On définit l'URL de ton FastAPI
const API_URL = "http://localhost:8000";

/**
 * Le dataProvider de Refine pour Simple REST utilise Axios en interne.
 * En ne passant que l'URL, il gère lui-même l'instance Axios proprement.
 */
export const dataProvider = dataProviderSimpleRest(API_URL);