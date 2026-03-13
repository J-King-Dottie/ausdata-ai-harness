import { createClient } from "@supabase/supabase-js";

const FALLBACK_SUPABASE_URL = "https://twylbietvtzyhefvpjqy.supabase.co";
const FALLBACK_SUPABASE_ANON_KEY = "sb_publishable_efc5Ef7rAN3QB_F9azx3Fg_0miAxF8G";

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL || FALLBACK_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY || FALLBACK_SUPABASE_ANON_KEY;

export const supabase = createClient(supabaseUrl, supabaseAnonKey, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,
  },
});
