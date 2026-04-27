import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';
import { sanitizeNumber, sanitizeString, sanitizeJson } from '../../utils/sql-sanitizer.js';

/**
 * Handler for Dashboard Operations
 * Uses the Metabase REST API for all dashboard mutations.
 */
export class DashboardDirectHandler {
    constructor(metabaseClient, metadataHandler) {
        this.metabaseClient = metabaseClient;
        this.metadataHandler = metadataHandler;
    }

    /**
     * Batch Update Dashboard Layout via Metabase API
     * GET /api/dashboard/{id} to fetch current dashcards (each with a real integer id),
     * then PUT /api/dashboard/{id} with the full updated dashcards array.
     */
    async handleUpdateLayoutSql(args) {
        const { dashboard_id, updates } = args;

        if (!updates || updates.length === 0) {
            return {
                content: [{ type: 'text', text: 'No layout updates provided — nothing to do.' }]
            };
        }

        logger.info(`Updating layout for ${updates.length} cards on dashboard ${dashboard_id} via API`);

        // 1. Fetch the current dashboard to get real dashcard IDs and full objects.
        let dashboard;
        try {
            dashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);
        } catch (error) {
            throw new McpError(ErrorCode.InternalError, `Failed to fetch dashboard ${dashboard_id}: ${error.message}`);
        }

        const dashcards = dashboard.dashcards || dashboard.ordered_cards || [];

        // 2. Build a lookup map from card_id -> dashcard for efficient access.
        //    There may be multiple dashcards with the same card_id (same question added
        //    more than once), so we track which ones have already been updated to avoid
        //    applying the same update twice.
        const byCardId = {};
        for (const dc of dashcards) {
            if (dc.card_id != null) {
                if (!byCardId[dc.card_id]) byCardId[dc.card_id] = [];
                byCardId[dc.card_id].push(dc);
            }
        }

        const applied = [];
        const errors = [];

        // 3. Apply each requested position update to the matching dashcard in-place.
        for (const update of updates) {
            if (!update.card_id) continue;

            const candidates = byCardId[update.card_id];
            if (!candidates || candidates.length === 0) {
                errors.push(`Card ${update.card_id} not found on dashboard`);
                continue;
            }

            // Use the first unmodified candidate (or the only one available).
            const dc = candidates.shift();

            if (update.row !== undefined) dc.row = update.row;
            if (update.col !== undefined) dc.col = update.col;
            if (update.size_x !== undefined) dc.size_x = update.size_x;
            if (update.size_y !== undefined) dc.size_y = update.size_y;

            applied.push(update.card_id);
        }

        // 4. PUT the updated dashcards array back.
        // Strip nested card objects — they are read-only in the GET response and
        // sending them back in a PUT can cause Metabase to reject the payload.
        const minimalDashcards = dashcards.map(dc => ({
            id: dc.id,
            card_id: dc.card_id,
            row: dc.row,
            col: dc.col,
            size_x: dc.size_x,
            size_y: dc.size_y,
            series: dc.series || [],
            parameter_mappings: dc.parameter_mappings || [],
            visualization_settings: dc.visualization_settings || {}
        }));

        try {
            await this.metabaseClient.request('PUT', `/api/dashboard/${dashboard_id}`, {
                dashcards: minimalDashcards
            });
        } catch (error) {
            throw new McpError(ErrorCode.InternalError, `Failed to PUT dashboard layout: ${error.message}`);
        }

        return {
            content: [{
                type: 'text',
                text: `**Layout Update Results**\nDashboard: ${dashboard_id}\nUpdated: ${applied.length}\nErrors: ${errors.length}${errors.length ? '\n\n' + errors.join('\n') : ''}`
            }]
        };
    }

    /**
     * Create Parametric Native SQL Question
     * Constructs the complex dataset_query JSON and inserts directly
     */
    async handleCreateParametricQuestionSql(args) {
        const { name, description, database_id, query_sql, parameters, collection_id } = args;

        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        // Construct Template Tags
        const templateTags = {};
        if (parameters && Array.isArray(parameters)) {
            for (const param of parameters) {
                const tagId = `tag_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
                templateTags[param.name] = {
                    "id": tagId,
                    "name": param.name,
                    "display-name": param.display_name || param.name,
                    "type": param.type || "text",
                    "default": param.default || null,
                    "required": param.required || false
                };
                if (param.widget_type) {
                    templateTags[param.name]["widget-type"] = param.widget_type;
                }
            }
        }

        const datasetQuery = {
            "type": "native",
            "native": {
                "query": query_sql,
                "template-tags": templateTags
            },
            "database": database_id
        };

        const safeName = sanitizeString(name);
        const safeDesc = sanitizeString(description || '');
        const safeQueryJson = sanitizeJson(datasetQuery);
        const safeDbId = sanitizeNumber(database_id);
        const collectionVal = collection_id ? sanitizeNumber(collection_id) : 'NULL';
        const creatorId = 1;

        const sql = `
            INSERT INTO report_card 
            (name, description, display, dataset_query, visualization_settings, 
             creator_id, database_id, query_type, created_at, updated_at, 
             collection_id, type, parameters, archived)
            VALUES 
            (
                '${safeName}',
                '${safeDesc}',
                'table',
                '${safeQueryJson}',
                '{}',
                ${creatorId},
                ${safeDbId},
                'native',
                NOW(),
                NOW(),
                ${collectionVal},
                'question',
                '[]',
                false
            )
        `;

        try {
            await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
            return {
                content: [{
                    type: 'text',
                    text: `✅ **Parametric Question Created**\n\nName: ${name}\nDB Source: ${database_id}\nParameters: ${Object.keys(templateTags).length}\n\n*Note: Use 'meta_advanced_search' to find the new Card ID.*`
                }]
            };
        } catch (error) {
            logger.error(`Failed to create parametric question: ${error.message}`);
            throw new McpError(ErrorCode.InternalError, `Failed to create question via SQL: ${error.message}`);
        }
    }

    /**
     * Link Dashboard Filter to Card Parameter via Metabase API
     * GET /api/dashboard/{id} to fetch current dashcards, update parameter_mappings
     * on the matching dashcard, then PUT /api/dashboard/{id} with full dashcards array.
     */
    async handleLinkDashboardFilter(args) {
        const { dashboard_id, card_id, dashcard_id, mappings } = args;

        logger.info(`Linking filter on dashboard ${dashboard_id} card ${card_id} via API`);

        // 1. Fetch current dashboard.
        let dashboard;
        try {
            dashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);
        } catch (error) {
            return { content: [{ type: 'text', text: `❌ Failed to fetch dashboard ${dashboard_id}: ${error.message}` }] };
        }

        const dashcards = dashboard.dashcards || dashboard.ordered_cards || [];

        // 2. Find the target dashcard.
        // Prefer dashcard_id (the dashcard's own integer id) so callers can disambiguate
        // when the same question appears on the dashboard more than once.
        let targetDashcard;
        if (dashcard_id != null) {
            targetDashcard = dashcards.find(dc => dc.id === dashcard_id);
            if (!targetDashcard) {
                return { content: [{ type: 'text', text: `❌ Dashcard ${dashcard_id} not found on dashboard ${dashboard_id}` }] };
            }
        } else {
            targetDashcard = dashcards.find(dc => dc.card_id === card_id);
            if (!targetDashcard) {
                return { content: [{ type: 'text', text: `❌ Card ${card_id} not found on dashboard ${dashboard_id}` }] };
            }
        }

        // 3. Build the new parameter_mappings array.
        //
        // target_type semantics:
        //   'variable'  — plain SQL template tag (text/number/date variable)
        //                 target: ["variable", ["template-tag", "tag_name"]]
        //   'dimension' — field-filter (dimension) SQL template tag
        //                 target: ["dimension", ["template-tag", "tag_name"]]
        //   'field'     — MBQL structured question field reference
        //                 target: ["dimension", ["field", field_id_integer, null]]
        //
        // The 'dimension' vs 'field' split is critical: native SQL questions use
        // template-tag references; MBQL questions use field ID references.
        const errors = [];
        const mappingArray = mappings.map(m => {
            const mapObj = {
                "parameter_id": m.parameter_id,
                "card_id": card_id,
                "target": null
            };

            if (m.target_type === 'variable') {
                mapObj.target = ["variable", ["template-tag", m.target_value]];
            } else if (m.target_type === 'dimension') {
                // Field-filter template tag on a native SQL question.
                mapObj.target = ["dimension", ["template-tag", m.target_value]];
            } else if (m.target_type === 'field') {
                // MBQL field reference (structured / GUI-built questions).
                const fieldId = parseInt(m.target_value, 10);
                if (isNaN(fieldId)) {
                    errors.push(`target_value '${m.target_value}' is not a valid field ID integer for mapping ${m.parameter_id}`);
                    return null;
                }
                mapObj.target = ["dimension", ["field", fieldId, null]];
            } else {
                errors.push(`Unknown target_type '${m.target_type}' for mapping ${m.parameter_id}`);
                return null;
            }
            return mapObj;
        }).filter(m => m !== null);

        if (errors.length > 0 && mappingArray.length === 0) {
            return { content: [{ type: 'text', text: `❌ All mappings failed validation:\n${errors.join('\n')}` }] };
        }

        // 4. Merge with any existing mappings (replace any that share parameter_id).
        const existingMappings = targetDashcard.parameter_mappings || [];
        const incomingIds = new Set(mappingArray.map(m => m.parameter_id));
        const retained = existingMappings.filter(m => !incomingIds.has(m.parameter_id));
        targetDashcard.parameter_mappings = [...retained, ...mappingArray];

        // 5. PUT the full updated dashcards array back.
        // Strip the nested card object from each dashcard — it's read-only data from
        // the GET response and sending it back can cause Metabase to reject the request.
        const minimalDashcards = dashcards.map(dc => ({
            id: dc.id,
            card_id: dc.card_id,
            row: dc.row,
            col: dc.col,
            size_x: dc.size_x,
            size_y: dc.size_y,
            series: dc.series || [],
            parameter_mappings: dc.parameter_mappings || [],
            visualization_settings: dc.visualization_settings || {}
        }));

        try {
            await this.metabaseClient.request('PUT', `/api/dashboard/${dashboard_id}`, {
                dashcards: minimalDashcards
            });
        } catch (error) {
            return { content: [{ type: 'text', text: `❌ Failed to PUT dashboard filter mappings: ${error.message}` }] };
        }

        const warnText = errors.length > 0 ? `\n⚠️ Some mappings skipped:\n${errors.join('\n')}` : '';
        return {
            content: [{
                type: 'text',
                text: `✅ Filter linked\nDashboard: ${dashboard_id}\nCard: ${card_id} (dashcard: ${targetDashcard.id})\nMappings applied: ${mappingArray.length}${warnText}`
            }]
        };
    }
}
