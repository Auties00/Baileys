export interface Contact {
    id: string
    lid?: string
    /** name of the contact, you have saved on your WA */
    name?: string
    /** name of the contact, the contact has set on their own on WA */
    notify?: string
    /** I have no idea */
    verifiedName?: string
    // Baileys Added
    /**
     * Url of the profile picture of the contact
     *
     * 'changed' => if the profile picture has changed
     * null => if the profile picture has not been set (default profile picture)
     * any other string => url of the profile picture
     */
    imgUrl?: string | null
    status?: string
}

export type ContactAction = {
    fistName: string
    fullName: string
    saveOnPrimaryAddressbook: boolean
}