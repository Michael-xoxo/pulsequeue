import React, { createContext, useContext, useState, useCallback } from 'react';

const AuthContext = createContext(null);

const TOKEN_STORAGE_KEY = 'pulsequeue_token';
const USER_STORAGE_KEY = 'pulsequeue_user';

export function AuthProvider({ children }) {
  const [token, setToken] = useState(() => localStorage.getItem(TOKEN_STORAGE_KEY) || '');
  const [user, setUser] = useState(() => {
    try {
      const raw = localStorage.getItem(USER_STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  });
  const [authError, setAuthError] = useState('');
  const [loading, setLoading] = useState(false);

  const saveSession = useCallback((nextToken, nextUser) => {
    localStorage.setItem(TOKEN_STORAGE_KEY, nextToken);
    localStorage.setItem(USER_STORAGE_KEY, JSON.stringify(nextUser));
    setToken(nextToken);
    setUser(nextUser);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(TOKEN_STORAGE_KEY);
    localStorage.removeItem(USER_STORAGE_KEY);
    setToken('');
    setUser(null);
  }, []);

  const login = useCallback(
    async (email, password) => {
      setLoading(true);
      setAuthError('');
      try {
        const res = await fetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password }),
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data.message || 'Login failed');
        }
        const resolvedUser = data.user || { _id: data._id, name: data.name, email: data.email };
        saveSession(data.token, resolvedUser);
        return data;
      } catch (err) {
        setAuthError(err.message);
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [saveSession]
  );

  const register = useCallback(
    async (name, email, password) => {
      setLoading(true);
      setAuthError('');
      try {
        const res = await fetch('/api/auth/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, email, password }),
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data.message || 'Registration failed');
        }
        const resolvedUser = data.user || { _id: data._id, name: data.name, email: data.email };
        saveSession(data.token, resolvedUser);
        return data;
      } catch (err) {
        setAuthError(err.message);
        throw err;
      } finally {
        setLoading(false);
      }
    },
    [saveSession]
  );

  const authFetch = useCallback(
    async (url, options = {}) => {
      const headers = {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      };

      const response = await fetch(url, {
        ...options,
        headers,
      });

      if (response.status === 401) {
        logout();
      }

      return response;
    },
    [token, logout]
  );

  return (
    <AuthContext.Provider
      value={{
        token,
        user,
        loading,
        authError,
        isAuthenticated: Boolean(token),
        login,
        register,
        logout,
        authFetch,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return ctx;
}
