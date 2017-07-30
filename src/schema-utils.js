function getAttributeDefinition (fb, a) {
  if (!fb.schema.hasOwnProperty(a) || !fb.schema[a]) {
    throw new Error(`Attribute not found: ${a}`)
  }
  return fb.schema[a]
}

function isAttributeMultiValued (fb, a) {
  return !!getAttributeDefinition(fb, a)['_db/is-multi-valued']
}

function getAttributeFromHash (fb, h) {
  const hashes = fb.schema['_db/attribute-hashes'] || {}
  if (!hashes.hasOwnProperty(h) || !hashes[h]) {
    throw new Error(`Attribute not found for hash ${h}`)
  }
  return hashes[h]
}

function getTypeNameForAttribute (fb, a) {
  const typeName = getAttributeDefinition(fb, a)['_db/type'] || 'String'
  if (!fb.types.hasOwnProperty(typeName)) {
    throw new Error(`Attribute ${a} has an unknown type ${typeName}`)
  }
  return typeName
}

function isAttributeHashMultiValued (fb, h) {
  const a = getAttributeFromHash(fb, h)
  return isAttributeMultiValued(fb, a)
}

function getTypeForAttribute (fb, a) {
  const typeName = getTypeNameForAttribute(fb, a)
  return fb.types[typeName]
}

export {
  getAttributeFromHash,
  getTypeForAttribute,
  getTypeNameForAttribute,
  isAttributeHashMultiValued,
  isAttributeMultiValued
}
