import axios from 'axios';
import { logger } from '../utils/logger.js';
import { sanitizeNumber, sanitizeLikePattern } from '../utils/sql-sanitizer.js';

export class MetabaseClient {
  constructor(config) {
    this.baseURL = config.url;
    this.username = config.username;
    this.password = config.password;
    this.apiKey = config.apiKey;
    this.sessionToken = null;
    this.defaultQueryTimeout = config.queryTimeout || 60000; // 60 seconds default
    this.client = axios.create({
      baseURL: this.baseURL,
      timeout: this.defaultQueryTimeout,
      headers: {
        'Content-Type': 'application/json'
      }
    });
  }

  async authenticate() {
    try {
      // API Key varsa, session authentication yerine API key kullan
      if (this.apiKey) {
        this.client.defaults.headers['x-api-key'] = this.apiKey;
        logger.info('Using API key authentication for Metabase');
        return true;
      }

      // Fallback: Username/password authentication
      const response = await this.client.post('/api/session', {
        username: this.username,
        password: this.password
      });
      this.sessionToken = response.data.id;
      this.client.defaults.headers['X-Metabase-Session'] = this.sessionToken;
      logger.info('Successfully authenticated with Metabase');
      return true;
    } catch (error) {
      const statusCode = error.response?.status;
      const errorDetail = error.response?.data?.message || error.message;

      let errorMessage = 'Failed to authenticate with Metabase';
      if (statusCode === 401) {
        errorMessage = 'Invalid username or password';
      } else if (statusCode === 403) {
        errorMessage = 'Access forbidden - check API key or permissions';
      } else if (error.code === 'ECONNREFUSED') {
        errorMessage = `Cannot connect to Metabase at ${this.baseURL}`;
      } else if (error.code === 'ENOTFOUND') {
        errorMessage = `Metabase host not found: ${this.baseURL}`;
      }

      logger.error(`Authentication failed: ${errorMessage}`, {
        statusCode,
        detail: errorDetail,
        url: this.baseURL
      });
      throw new Error(`${errorMessage}: ${errorDetail}`);
    }
  }

  /**
   * Generic request wrapper for Metabase API
   * Used by MCP handlers that need arbitrary API access
   */
  async request(method, endpoint, data = null, config = {}) {
    await this.ensureAuthenticated();

    // Normalize method
    const methodUpper = method.toUpperCase();

    const requestConfig = {
      ...config,
      method: methodUpper,
      url: endpoint,
      data: data
    };

    // For GET requests with data, move to params if not already set
    if (methodUpper === 'GET' && data && !requestConfig.params) {
      requestConfig.params = data;
      delete requestConfig.data;
    }

    try {
      const response = await this.client.request(requestConfig);
      return response.data;
    } catch (error) {
      // Build a diagnostic error message that surfaces what actually happened.
      // The previous one-liner ("Metabase API Error: <msg>") often resolved to
      // axios's generic "Request failed with status code 4xx" when Metabase
      // returned a non-standard error shape (HTML page, {error: ...}, etc.),
      // making it impossible to distinguish 401/403/404/500 from the consumer.
      const status = error.response?.status;
      const data = error.response?.data;

      let detail;
      if (data && typeof data === 'object') {
        // Metabase typically returns {message: ...} or {error: ...} or {detail: ...}.
        detail = data.message || data.error || data.detail
          || JSON.stringify(data).slice(0, 200);
      } else if (typeof data === 'string' && data.length) {
        // HTML error page (e.g. Cloudflare Access challenge) or plain text.
        detail = data.slice(0, 200).replace(/\s+/g, ' ').trim();
      } else if (error.code === 'ECONNABORTED' || /timeout/i.test(error.message || '')) {
        detail = `request timed out after ${requestConfig.timeout || this.defaultQueryTimeout}ms`;
      } else if (error.code) {
        detail = `${error.code}: ${error.message}`;
      } else {
        detail = error.message || 'unknown error';
      }

      const where = `${methodUpper} ${endpoint}`;
      const tag = status ? `HTTP ${status}` : (error.code || 'no response');
      const fullMsg = `Metabase API Error: ${tag} ${where} — ${detail}`;

      logger.error('API Request Failed', { where, status, code: error.code, detail });
      throw new Error(fullMsg);
    }
  }

  // Database Operations
  async getDatabases() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/database');
    // Handle both formats: array or {data: array}
    if (Array.isArray(response.data)) {
      return response.data;
    } else if (response.data && response.data.data) {
      return response.data.data;
    }
    return [];
  }

  async getDatabase(id) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/database/${id}`);
    return response.data;
  }

  async getDatabaseConnectionInfo(id) {
    await this.ensureAuthenticated();

    // Try real credentials from MetabaseappDB first
    try {
      const realCredentials = await this.getRealCredentials(id);
      if (realCredentials) {
        return realCredentials;
      }
    } catch (error) {
      logger.warn('Could not get real credentials, using API response:', error.message);
    }

    // Fallback: Normal API response
    const response = await this.client.get(`/api/database/${id}`);
    const db = response.data;

    return {
      id: db.id,
      name: db.name,
      engine: db.engine,
      host: db.details?.host,
      port: db.details?.port,
      dbname: db.details?.dbname || db.details?.db,
      user: db.details?.user,
      password: db.details?.password ? '***REDACTED***' : null,
      ssl: db.details?.ssl,
      additional_options: db.details?.['additional-options'],
      tunnel_enabled: db.details?.['tunnel-enabled'],
      connection_string: this.buildConnectionString(db)
    };
  }

  async getRealCredentials(databaseId) {
    const safeDbId = sanitizeNumber(databaseId);
    const query = `
      SELECT name, engine, details
      FROM metabase_database 
      WHERE id = ${safeDbId}
    `;

    const result = await this.executeNativeQuery(6, query, { enforcePrefix: false }); // MetabaseappDB

    if (result.data.rows.length > 0) {
      const [name, engine, details] = result.data.rows[0];
      const detailsObj = JSON.parse(details);

      return {
        id: databaseId,
        name: name,
        engine: engine,
        host: detailsObj.host,
        port: detailsObj.port,
        dbname: detailsObj.dbname,
        user: detailsObj.user,
        password: detailsObj.password ? '***REDACTED***' : null,
        ssl: detailsObj.ssl || false,
        additional_options: detailsObj['additional-options'],
        tunnel_enabled: detailsObj['tunnel-enabled'] || false
      };
    }

    return null;
  }

  buildConnectionString(db) {
    const details = db.details;

    switch (db.engine) {
      case 'postgres':
        return `postgresql://${details.user}:***@${details.host}:${details.port}/${details.dbname}`;
      case 'mysql':
        return `mysql://${details.user}:***@${details.host}:${details.port}/${details.dbname}`;
      case 'h2':
        return details.db;
      default:
        return null;
    }
  }

  async getDatabaseSchemas(databaseId) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/database/${databaseId}/schemas`);
    return response.data;
  }

  async getDatabaseTables(databaseId, schemaName = null) {
    await this.ensureAuthenticated();
    let endpoint = `/api/database/${databaseId}/metadata`;
    if (schemaName) {
      endpoint += `?schema=${encodeURIComponent(schemaName)}`;
    }
    const response = await this.client.get(endpoint);
    return response.data.tables;
  }

  async getTable(tableId) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/table/${tableId}`);
    return response.data;
  }

  async getTableFields(tableId) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/table/${tableId}/query_metadata`);
    return response.data.fields;
  }

  async updateField(fieldId, updates) {
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/field/${fieldId}`, updates);
    return response.data;
  }

  async getModelFields(modelId) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/card/${modelId}/query_metadata`);
    return response.data.fields || [];
  }

  // Model Operations
  async getCollections() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/collection');
    return response.data;
  }

  async createCollection(name, description, parentId = null) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/collection', {
      name,
      description,
      parent_id: parentId,
      color: '#509EE3'
    });
    return response.data;
  }

  async getModels() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/card', {
      params: { f: 'model' }
    });
    return response.data;
  }

  async createModel(model) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/card', {
      ...model,
      type: 'model',
      display: 'table'
    });
    return response.data;
  }

  // Question Operations
  async getQuestions(collectionId = null) {
    await this.ensureAuthenticated();
    const params = collectionId ? { collection_id: collectionId } : {};
    const response = await this.client.get('/api/card', { params });
    return response.data;
  }

  async createQuestion(question) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/card', {
      ...question,
      display: question.display || 'table',
      visualization_settings: question.visualization_settings || {}
    });
    return response.data;
  }

  async createParametricQuestion(questionData) {
    await this.ensureAuthenticated();

    // Build native query with parameters
    const nativeQuery = {
      type: 'native',
      native: {
        query: questionData.sql,
        "template-tags": {}
      },
      database: questionData.database_id
    };

    // Add parameter template tags
    if (questionData.parameters) {
      for (const param of questionData.parameters) {
        const tag = {
          id: param.name,
          name: param.name,
          "display-name": param.display_name,
          required: param.required || false,
          default: param.default_value ?? null
        };

        if (param.field_id) {
          // Field-filter (dimension) parameter — binds to a specific Metabase field.
          // Metabase requires type:"dimension", a dimension array, and a widget-type.
          tag.type = "dimension";
          tag.dimension = ["field", param.field_id, null];
          // Default widget-type is "date/all-options" — the ONLY widget-type that
          // accepts BOTH absolute date ranges AND relative strings (past13weeks,
          // thismonth, etc.).  "date/range" rejects relative strings with a 500
          // when the dashboard passes a date filter default value.  Caller can
          // override via param.widget_type.
          //
          // We deliberately do NOT fall back to param.type here.  param.type is
          // the *parameter type* namespace ("date", "text", "number", "category")
          // which doesn't overlap with widget-type values — earlier code conflated
          // them, producing tags like widget-type:"date" that crashed Metabase.
          tag["widget-type"] = param.widget_type || "date/all-options";
        } else {
          // Plain variable substitution (text, number, date, category, etc.)
          tag.type = param.type || "text";
          if (param.widget_type) {
            tag["widget-type"] = param.widget_type;
          }
        }

        nativeQuery.native["template-tags"][param.name] = tag;
      }
    }

    // NOTE: visualization_settings are intentionally NOT applied at creation time.
    //
    // Metabase v0.60 triggers a result_metadata recompute (query execution) when
    // visualization_settings containing graph.dimensions / graph.metrics are
    // included in the POST body — the POST blocks until the query finishes,
    // which can take 30–120 s and kills the MCP stdio transport.
    //
    // A follow-up PUT /api/card/:id with only {visualization_settings: ...} and
    // NO dataset_query field clears the stored dataset_query in Metabase v0.60
    // (PUT treats missing required fields as null, corrupting the card's SQL).
    //
    // Safe pattern after creation:
    //   1. Use mb_visualization_settings to set graph.dimensions / graph.metrics
    //      on the QUESTION itself (PUT /api/card/:id with the full card body)
    //   2. Use mb_dashboard_refresh_viz to propagate question viz settings to
    //      dashcards after the card is added to a dashboard.
    //
    // If visualization_settings were passed, they are stored in the returned
    // object so the caller knows they need to be applied separately.
    const pendingVizSettings = questionData.visualization_settings &&
      Object.keys(questionData.visualization_settings).length > 0
        ? questionData.visualization_settings
        : null;

    const cardBody = {
      name: questionData.name,
      description: questionData.description,
      dataset_query: nativeQuery,
      display: questionData.visualization || 'table',
      visualization_settings: {},
      collection_id: questionData.collection_id || null
    };

    const response = await this.client.post('/api/card', cardBody);
    const card = response.data;

    // Surface pending viz settings to the caller without applying them.
    if (pendingVizSettings) {
      card._pendingVizSettings = pendingVizSettings;
    }

    return card;
  }

  async updateQuestion(id, updates) {
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/card/${id}`, updates);
    return response.data;
  }

  async runQuery(query) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/dataset', query);
    return response.data;
  }

  // SQL Operations
  async executeNativeQuery(databaseId, sql, options = {}) {
    await this.ensureAuthenticated();

    // Security check - DDL operations require prefix
    if (options.enforcePrefix !== false && this.isDDLOperation(sql)) {
      this.validateDDLPrefix(sql);
    }

    // DDL operations use different endpoint
    if (this.isDDLOperation(sql)) {
      return await this.executeDDLOperation(databaseId, sql);
    }

    const query = {
      database: databaseId,
      type: 'native',
      native: {
        query: sql
      }
    };
    return await this.runQuery(query);
  }

  /**
   * Execute query with custom timeout and abort signal
   * Used for async query management
   */
  async executeNativeQueryWithTimeout(databaseId, sql, timeoutMs, abortSignal = null) {
    await this.ensureAuthenticated();

    const query = {
      database: databaseId,
      type: 'native',
      native: {
        query: sql
      }
    };

    const config = {
      timeout: timeoutMs
    };

    // Add abort signal if provided
    if (abortSignal) {
      config.signal = abortSignal;
    }

    try {
      const response = await this.client.post('/api/dataset', query, config);
      return response.data;
    } catch (error) {
      if (error.name === 'AbortError' || error.code === 'ERR_CANCELED') {
        throw new Error('Query cancelled');
      }
      if (error.code === 'ECONNABORTED' || error.message.includes('timeout')) {
        throw new Error(`Query timed out after ${timeoutMs / 1000} seconds`);
      }
      throw error;
    }
  }

  /**
   * Cancel a running query on PostgreSQL database
   * This sends pg_cancel_backend to stop the query on the server side
   */
  async cancelPostgresQuery(databaseId, queryMarker) {
    try {
      const safeMarker = sanitizeLikePattern(queryMarker);
      const cancelSql = `
        SELECT pg_cancel_backend(pid)
        FROM pg_stat_activity
        WHERE query LIKE '%${safeMarker}%'
          AND state = 'active'
          AND pid != pg_backend_pid()
      `;

      await this.executeNativeQuery(databaseId, cancelSql, { enforcePrefix: false });
      logger.info(`Attempted to cancel query with marker: ${queryMarker}`);
      return true;
    } catch (error) {
      logger.warn(`Failed to cancel query: ${error.message}`);
      return false;
    }
  }

  async executeDDLOperation(databaseId, sql) {
    // POST /api/action/execute is not an ad-hoc SQL endpoint and was never a
    // supported DDL path. Metabase v0.60 does not provide any API for running
    // DDL statements directly. Use the dedicated schema-creation tools
    // (db_table_create, db_view_create, etc.) which call the correct endpoints,
    // or execute DDL directly against the source database.
    throw new Error(
      'DDL operations are not supported on this Metabase version. ' +
      'Use db_table_create, db_view_create, db_matview_create, or db_index_create tools instead.'
    );
  }

  isDDLOperation(sql) {
    const upperSQL = sql.toUpperCase().trim();
    return upperSQL.startsWith('CREATE TABLE') ||
      upperSQL.startsWith('CREATE VIEW') ||
      upperSQL.startsWith('CREATE MATERIALIZED VIEW') ||
      upperSQL.startsWith('CREATE INDEX') ||
      upperSQL.startsWith('DROP TABLE') ||
      upperSQL.startsWith('DROP VIEW') ||
      upperSQL.startsWith('DROP MATERIALIZED VIEW') ||
      upperSQL.startsWith('DROP INDEX');
  }

  validateDDLPrefix(sql) {
    const upperSQL = sql.toUpperCase();

    // CREATE operations için prefix kontrolü
    if (upperSQL.includes('CREATE TABLE') || upperSQL.includes('CREATE VIEW') ||
      upperSQL.includes('CREATE MATERIALIZED VIEW') || upperSQL.includes('CREATE INDEX')) {
      if (!sql.toLowerCase().includes('claude_ai_')) {
        throw new Error('DDL operations must use claude_ai_ prefix for object names');
      }
    }

    // DROP operations için sadece prefix'li objelere izin
    if (upperSQL.includes('DROP TABLE') || upperSQL.includes('DROP VIEW') ||
      upperSQL.includes('DROP MATERIALIZED VIEW') || upperSQL.includes('DROP INDEX')) {
      if (!sql.toLowerCase().includes('claude_ai_')) {
        throw new Error('Can only drop objects with claude_ai_ prefix');
      }
    }
  }

  async createSQLQuestion(name, description, databaseId, sql, collectionId) {
    const question = {
      name,
      description,
      database_id: databaseId,
      collection_id: collectionId,
      dataset_query: {
        database: databaseId,
        type: 'native',
        native: {
          query: sql
        }
      }
    };
    return await this.createQuestion(question);
  }

  // Metric Operations
  // NOTE: /api/metric and /api/segment were removed in Metabase v0.49+.
  // Metrics are now ordinary cards with type='metric' stored via /api/card.
  async getMetrics() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/card', { params: { f: 'metric' } });
    return response.data;
  }

  async createMetric(args) {
    await this.ensureAuthenticated();

    // Look up the table to determine which database it belongs to.
    let databaseId;
    try {
      const table = await this.request('GET', `/api/table/${args.table_id}`);
      databaseId = table.db_id;
    } catch (e) {
      throw new Error(`Could not look up table ${args.table_id}: ${e.message}`);
    }

    // Build MBQL aggregation clause.
    const agg = args.aggregation || { type: 'count' };
    let aggregationClause;
    if (agg.type === 'count') {
      aggregationClause = ['count'];
    } else if (agg.field_id) {
      aggregationClause = [agg.type, ['field', agg.field_id, null]];
    } else {
      throw new Error(`Aggregation type '${agg.type}' requires a field_id`);
    }

    // Build MBQL query.
    const query = {
      'source-table': args.table_id,
      aggregation: [aggregationClause]
    };

    // Build MBQL filter clause from optional filters array.
    if (args.filters && args.filters.length > 0) {
      const filterClauses = args.filters.map(f => {
        if (f.operator === 'is-null') return ['is-null', ['field', f.field_id, null]];
        if (f.operator === 'not-null') return ['not-null', ['field', f.field_id, null]];
        return [f.operator, ['field', f.field_id, null], f.value];
      });
      query.filter = filterClauses.length === 1 ? filterClauses[0] : ['and', ...filterClauses];
    }

    const cardPayload = {
      name: args.name,
      description: args.description,
      type: 'metric',
      dataset_query: {
        database: databaseId,
        type: 'query',
        query
      },
      display: 'table',
      visualization_settings: {}
    };

    const response = await this.client.post('/api/card', cardPayload);
    return response.data;
  }

  async updateMetric(id, updates) {
    // In Metabase v0.49+ metrics are cards; update via PUT /api/card/:id.
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/card/${id}`, updates);
    return response.data;
  }

  // Dashboard Operations
  async getDashboards() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/dashboard');
    return response.data;
  }

  async getDashboard(id) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/dashboard/${id}`);
    return response.data;
  }

  async createDashboard(dashboard) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/dashboard', {
      name: dashboard.name,
      description: dashboard.description,
      collection_id: dashboard.collection_id
    });
    return response.data;
  }

  async addCardToDashboard(dashboardId, cardId, options = {}) {
    // Metabase v0.60 removed POST /api/dashboard/:id/cards.
    // The correct API is PUT /api/dashboard/:id with a full "dashcards" array.
    // New cards use a negative id (e.g. -1); existing cards keep their real id.
    await this.ensureAuthenticated();

    try {
      // 1. Fetch the current dashboard to get existing dashcards.
      const dashboard = await this.request('GET', `/api/dashboard/${dashboardId}`);
      const existingCards = dashboard.dashcards || dashboard.ordered_cards || [];

      // 2. Build the new dashcard entry with a unique negative id.
      const minId = existingCards.reduce((min, c) => Math.min(min, c.id || 0), 0);
      const newCardId = minId - 1;

      const newDashcard = {
        id: newCardId,
        card_id: cardId,
        row: options.row !== undefined ? options.row : 0,
        col: options.col !== undefined ? options.col : 0,
        size_x: options.sizeX || options.size_x || 4,
        size_y: options.sizeY || options.size_y || 4,
        series: [],
        parameter_mappings: options.parameter_mappings || [],
        visualization_settings: options.visualization_settings || {}
      };

      // 3. PUT the full dashcards array (existing + new).
      const updatedDashboard = await this.request('PUT', `/api/dashboard/${dashboardId}`, {
        dashcards: [...existingCards, newDashcard]
      });

      // Return the newly created dashcard (server assigns real id).
      const createdCards = updatedDashboard.dashcards || updatedDashboard.ordered_cards || [];
      const created = createdCards.find(c => c.card_id === cardId && !existingCards.some(e => e.id === c.id));
      return created || newDashcard;

    } catch (error) {
      throw new Error(`Failed to add card to dashboard: ${error.message}`);
    }
  }

  async addCardToDashboardDirect(dashboardId, cardId, options = {}) {
    // Direct database insertion as fallback
    const query = `
      INSERT INTO report_dashboardcard (
        created_at, 
        updated_at, 
        size_x, 
        size_y, 
        row, 
        col, 
        card_id, 
        dashboard_id,
        parameter_mappings,
        visualization_settings
      ) VALUES (
        NOW(),
        NOW(),
        $1, $2, $3, $4, $5, $6, $7, $8
      ) RETURNING id
    `;

    const values = [
      options.sizeX || 4,
      options.sizeY || 4,
      options.row || 0,
      options.col || 0,
      cardId,
      dashboardId,
      JSON.stringify(options.parameter_mappings || []),
      JSON.stringify(options.visualization_settings || {})
    ];

    // This would need database connection - placeholder for now
    return { id: 'inserted_via_sql', method: 'direct_sql' };
  }

  async updateDashboard(id, updates) {
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/dashboard/${id}`, updates);
    return response.data;
  }

  async addDashboardFilter(dashboardId, filter) {
    await this.ensureAuthenticated();

    // Get current dashboard to add filter
    const dashboard = await this.client.get(`/api/dashboard/${dashboardId}`);
    const currentFilters = dashboard.data.parameters || [];

    // Create proper Metabase filter format
    const newFilter = {
      id: this.generateFilterId(),
      name: filter.name,
      slug: filter.slug || filter.name.toLowerCase().replace(/\s+/g, '_'),
      type: filter.type,
      sectionId: "filters"
    };

    // Add type-specific properties
    if (filter.type === 'date/range') {
      newFilter.default = null;
    } else if (filter.default_value !== undefined) {
      newFilter.default = filter.default_value;
    }

    const updatedFilters = [...currentFilters, newFilter];

    return await this.updateDashboard(dashboardId, {
      parameters: updatedFilters
    });
  }

  generateFilterId() {
    return Math.random().toString(36).substr(2, 9);
  }

  // Segment Operations
  async getSegments(tableId) {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/segment', {
      params: { table_id: tableId }
    });
    return response.data;
  }

  async createSegment(segment) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/segment', segment);
    return response.data;
  }

  // Helper Methods
  async ensureAuthenticated() {
    // API key mode: always set, no expiry
    if (this.apiKey) return;

    // Session token mode: check if set
    if (!this.sessionToken) {
      await this.authenticate();
    }
  }

  /**
   * Retry a request on 401 (session expired)
   * Re-authenticates and retries once
   */
  async requestWithRetry(fn) {
    try {
      return await fn();
    } catch (error) {
      if (error.response?.status === 401 && !this.apiKey) {
        logger.warn('Session expired, re-authenticating...');
        this.sessionToken = null;
        await this.authenticate();
        return await fn();
      }
      throw error;
    }
  }

  async testConnection() {
    try {
      await this.authenticate();
      const databases = await this.getDatabases();
      const dbCount = Array.isArray(databases) ? databases.length : 0;
      logger.info(`Connected to Metabase. Found ${dbCount} databases.`);
      return true;
    } catch (error) {
      logger.error('Connection test failed:', error.message);
      return false;
    }
  }
}